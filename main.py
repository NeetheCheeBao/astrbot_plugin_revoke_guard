import asyncio
import time
import traceback
import json
import base64
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any
import aiohttp
import os
import shutil

from astrbot.api import logger
from astrbot.api.star import StarTools
from astrbot.api import message_components as Comp
from astrbot.api.star import Context, Star
from astrbot.core.message.message_event_result import MessageChain
from astrbot.api.platform import MessageType
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
def build_cache_task_key(group_id: str, message_id: str) -> str:
    return f"{group_id}:{message_id}"


@dataclass
class CacheTaskState:
    task: asyncio.Task | None
    done: asyncio.Event = field(default_factory=asyncio.Event)
    result: dict[str, Any] | None = None
    error: str | None = None
    started_at: float = field(default_factory=time.time)


def get_private_unified_msg_origin(user_id: str, platform: str = "aiocqhttp") -> str:
    return f"{platform}:FriendMessage:{user_id}"


async def delayed_delete(delay: int, path: Path):
    await asyncio.sleep(delay)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        logger.error(f"[RevokeGuard] 删除文件失败 ({path}): {traceback.format_exc()}")


async def _cleanup_local_files(file_paths: List[str]):
    if not file_paths:
        return
    await asyncio.sleep(1)
    for abs_path in file_paths:
        try:
            os.remove(abs_path)
        except Exception as e:
            logger.error(f"[RevokeGuard] ❌ 清理本地文件失败 ({abs_path}): {e}")


def get_value(obj, key, default=None):
    try:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)
    except Exception:
        return default


COMPONENT_MAP = {
    "Plain": Comp.Plain,
    "Text": Comp.Plain,
    "Image": Comp.Image,
    "Face": Comp.Face,
    "At": Comp.At,
    "Video": Comp.Video,
    "Record": Comp.Record,
    "File": Comp.File,
    "Json": Comp.Json,
    "Reply": Comp.Reply,
}


def _serialize_components(components: list) -> List[Dict]:
    serialized_list = []
    for comp in components:
        try:
            comp_type_name = getattr(comp.type, "name", "unknown")
            if comp_type_name == "Reply":
                serialized_list.append(
                    {
                        "type": "Reply",
                        "id": str(getattr(comp, "id", "") or ""),
                    }
                )
                continue
            comp_dict = {
                k: v for k, v in comp.__dict__.items() if not k.startswith("_")
            }
            comp_dict["type"] = comp_type_name
            serialized_list.append(comp_dict)
        except Exception:
            serialized_list.append({"type": "Unknown", "data": f"<{str(comp)}>"})
    return serialized_list


def _deserialize_components(comp_dicts: List[Dict]) -> List:
    components = []
    for comp_dict in comp_dicts:
        data_to_construct = comp_dict.copy()
        comp_type_name = data_to_construct.pop("type", None)

        if not comp_type_name:
            logger.warning("[RevokeGuard] 反序列化时遇到缺少类型的组件字典，已跳过。")
            continue

        cls = COMPONENT_MAP.get(comp_type_name)
        if cls:
            try:
                if "file_" in data_to_construct:
                    data_to_construct["file"] = data_to_construct.pop("file_")

                components.append(cls(**data_to_construct))
            except Exception as e:
                logger.error(f"[RevokeGuard] 反序列化组件 {comp_type_name} 失败: {e}")
        else:
            if comp_type_name != "Forward":
                logger.warning(
                    f"[RevokeGuard] 反序列化时遇到未知组件类型 '{comp_type_name}'，已跳过。"
                )
    return components


def _build_original_image_urls(raw_message: dict) -> list[str | None]:
    """从 raw_message 中提取 Image 段的原始 HTTP URL 列表，顺序与消息段一致。"""
    urls = []
    if isinstance(raw_message, dict):
        msg_list = raw_message.get("message", [])
        if isinstance(msg_list, list):
            for seg in msg_list:
                if isinstance(seg, dict) and seg.get("type") == "image":
                    data = seg.get("data", {}) or {}
                    url = data.get("url", "") or ""
                    urls.append(
                        url if url.startswith(("http://", "https://")) else None
                    )
    return urls


async def _download_and_cache_image(
    session: aiohttp.ClientSession, component: Comp.Image, temp_path: Path
) -> str:
    image_url = getattr(component, "url", None)
    if not image_url:
        return None

    # 本地路径直接复制，兼容 AstrBot 预处理阶段已将 url 改写为本地缓存路径的场景
    if not image_url.startswith(("http://", "https://")):
        src = Path(image_url)
        if src.exists():
            file_name = f"forward_{int(time.time() * 1000)}{src.suffix or '.jpg'}"
            dest = temp_path / file_name
            try:
                shutil.copy2(str(src), str(dest))
                return str(dest.absolute())
            except Exception as e:
                logger.error(f"[RevokeGuard] ❌ 图片复制失败 ({image_url}): {e}")
                return None
        else:
            logger.warning(f"[RevokeGuard] 图片路径不存在: {image_url}")
            return None

    file_name = f"forward_{int(time.time() * 1000)}{Path(image_url).suffix or '.jpg'}"
    temp_file_path = temp_path / file_name
    try:
        headers = {"User-Agent": "Mozilla/5.0 ...", "Referer": "https://qzone.qq.com/"}
        async with session.get(image_url, headers=headers, timeout=15) as response:
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            if "image" not in content_type and "octet-stream" not in content_type:
                logger.warning(f"[RevokeGuard] 下载 URL 返回类型非图片: {content_type}")
                return None
            image_bytes = await response.read()
            with open(temp_file_path, "wb") as f:
                f.write(image_bytes)
        return str(temp_file_path.absolute())
    except Exception as e:
        logger.error(f"[RevokeGuard] ❌ 图片下载或保存失败 ({image_url}): {e}")
        if temp_file_path.exists():
            os.remove(temp_file_path)
        return None


async def _process_component_and_get_gocq_part(
    comp,
    session: aiohttp.ClientSession,
    temp_path: Path,
    local_files_to_cleanup: List[str],
    local_file_map: Dict = None,
    show_qq_in_at: bool = False,
    include_reply: bool = False,
) -> List[Dict]:
    gocq_parts = []
    comp_type_name = getattr(comp.type, "name", "unknown")
    if comp_type_name == "Reply":
        if include_reply:
            reply_id = getattr(comp, "id", None)
            if reply_id is not None:
                gocq_parts.append(
                    {"type": "reply", "data": {"id": str(reply_id)}}
                )
    elif comp_type_name in ["Plain", "Text"]:
        text = getattr(comp, "text", "")
        if text:
            gocq_parts.append({"type": "text", "data": {"text": text}})
    elif comp_type_name == "Face":
        face_id = getattr(comp, "id", None)
        if face_id is not None:
            gocq_parts.append({"type": "face", "data": {"id": int(face_id)}})
    elif comp_type_name == "At":
        qq = getattr(comp, "qq", None)
        if qq:
            name = getattr(comp, "name", f"@{qq}")
            if show_qq_in_at:
                gocq_parts.append({"type": "text", "data": {"text": f"@{name}({qq}) "}})
            else:
                gocq_parts.append({"type": "text", "data": {"text": f"@{name} "}})
    elif comp_type_name == "Image":
        local_path = await _download_and_cache_image(session, comp, temp_path)
        if local_path:
            local_files_to_cleanup.append(local_path)
            try:
                with open(local_path, "rb") as image_file:
                    encoded_string = base64.b64encode(image_file.read()).decode("utf-8")
                gocq_parts.append(
                    {"type": "image", "data": {"file": f"base64://{encoded_string}"}}
                )
            except Exception as e:
                logger.error(f"[RevokeGuard] ❌ 图片转Base64失败: {e}")
                # 降级重试：如果Base64失败，尝试使用文件路径
                gocq_parts.append(
                    {"type": "image", "data": {"file": f"file:///{local_path}"}}
                )
        else:
            gocq_parts.append({"type": "text", "data": {"text": "[图片转发失败]"}})
    elif comp_type_name == "Video":
        cached_video_path_str = getattr(comp, "file", None)
        if cached_video_path_str and cached_video_path_str.startswith(
            "[视频过大未缓存:"
        ):
            gocq_parts.append({"type": "text", "data": {"text": cached_video_path_str}})
        elif cached_video_path_str and Path(cached_video_path_str).exists():
            absolute_path = str(Path(cached_video_path_str).absolute())
            gocq_parts.append(
                {"type": "video", "data": {"file": f"file:///{absolute_path}"}}
            )
        else:
            logger.error(
                f"[RevokeGuard] ❌ 准备发送视频时失败：缓存的视频文件已丢失，路径: {cached_video_path_str}"
            )
            gocq_parts.append(
                {"type": "text", "data": {"text": "[错误：撤回的视频文件已丢失]"}}
            )
    elif comp_type_name == "Record":
        cached_voice_path_str = getattr(comp, "file", None)
        if cached_voice_path_str and Path(cached_voice_path_str).exists():
            absolute_path = str(Path(cached_voice_path_str).absolute())
            try:
                with open(absolute_path, "rb") as voice_file:
                    encoded_string = base64.b64encode(voice_file.read()).decode("utf-8")
                gocq_parts.append(
                    {"type": "record", "data": {"file": f"base64://{encoded_string}"}}
                )
            except Exception as e:
                logger.error(f"[RevokeGuard] ❌ 语音转Base64失败: {e}")
                # 降级重试：如果Base64失败，尝试使用文件路径
                gocq_parts.append(
                    {"type": "record", "data": {"file": f"file:///{absolute_path}"}}
                )
        else:
            logger.error(
                f"[RevokeGuard] ❌ 准备发送语音时失败：缓存的语音文件已丢失，路径: {cached_voice_path_str}"
            )
            gocq_parts.append(
                {"type": "text", "data": {"text": "[错误：撤回的语音文件已丢失]"}}
            )
    elif comp_type_name == "File":
        unique_key = getattr(comp, "url", None)
        cached_file_path_str = (
            local_file_map.get(unique_key) if local_file_map and unique_key else None
        )

        original_filename = None
        if cached_file_path_str:
            if cached_file_path_str.startswith("[文件过大未缓存:"):
                gocq_parts.append(
                    {"type": "text", "data": {"text": cached_file_path_str}}
                )
                return gocq_parts
            try:
                original_filename = Path(cached_file_path_str).name.split("_", 1)[1]
            except IndexError:
                original_filename = Path(cached_file_path_str).name

        if cached_file_path_str and Path(cached_file_path_str).exists():
            absolute_path = str(Path(cached_file_path_str).absolute())
            gocq_parts.append(
                {
                    "type": "file",
                    "data": {
                        "file": f"file:///{absolute_path}",
                        "name": original_filename,
                    },
                }
            )
        else:
            logger.error(
                f"[RevokeGuard] ❌ 准备发送 File 时失败：缓存的文件已丢失。Key: {unique_key}"
            )
            gocq_parts.append(
                {
                    "type": "text",
                    "data": {
                        "text": f"[错误：撤回的文件 '{original_filename or ''}' 已丢失]"
                    },
                }
            )
    elif comp_type_name == "Forward":
        gocq_parts.append({"type": "text", "data": {"text": "[合并转发消息]"}})
    elif comp_type_name == "Json":
        json_data = getattr(comp, "data", "{}")
        if isinstance(json_data, dict):
            json_data_str = json.dumps(json_data, ensure_ascii=False)
        else:
            json_data_str = str(json_data)

        try:
            json.loads(json_data_str)
            gocq_part = {"type": "json", "data": {"data": json_data_str}}
            gocq_parts.append(gocq_part)
        except Exception as e:
            logger.error(
                f"[RevokeGuard] ❌ 处理 Json 组件失败，原始数据可能不是有效的 JSON: {e}"
            )
            gocq_parts.append(
                {"type": "text", "data": {"text": "[小程序转发失败，原始数据格式错误]"}}
            )

    return gocq_parts


class RevokeGuard(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        config = config or {}
        self.instance_id = "RevokeGuard"
        monitor = config.get("monitor", {}) or {}
        target = config.get("target", {}) or {}
        cache = config.get("cache", {}) or {}
        forward = config.get("forward", {}) or {}
        self.monitor_mode = str(
            monitor.get("monitor_mode")
            or ("白名单群聊" if monitor.get("monitor_groups") else "所有群聊")
        )
        self.whitelist_groups = [
            str(g)
            for g in (
                monitor.get("whitelist_groups")
                or monitor.get("monitor_groups")
                or []
            )
            if str(g).strip()
        ]
        self.blacklist_groups = [
            str(g) for g in (monitor.get("blacklist_groups") or []) if str(g).strip()
        ]
        # 黑名单用户：其撤回行为不触发通知（兼容旧字段 ignore_operators）
        self.ignore_operators = [
            str(o)
            for o in (
                monitor.get("blacklist_users")
                or monitor.get("ignore_operators")
                or []
            )
        ]
        self.ignore_keywords = [
            str(k).strip()
            for k in (monitor.get("ignore_keywords", []) or [])
            if str(k).strip()
        ]

        self.notify_mode = str(target.get("notify_mode") or "发送到群聊")
        self.target_receivers = [
            str(r) for r in target.get("target_receivers", []) or []
        ]
        self.target_groups = [str(g) for g in target.get("target_groups", []) or []]

        self.cache_expiration_time = int(cache.get("cache_expiration_time", 300))
        self.cache_pending_timeout = min(self.cache_expiration_time, 2)
        self.file_download_retry_attempts = 3
        self.file_download_retry_delay = 0.2
        self.file_size_threshold_mb = int(cache.get("file_size_threshold_mb", 60))

        relay_mode_raw = str(forward.get("relay_mode") or "").strip()
        relay_group_cfg = str(forward.get("forward_relay_group", "") or "").strip()
        # 兼容各种存储值 / 旧版 bool
        if (
            "群聊" in relay_mode_raw
            or relay_mode_raw in ("group", "relay_group", "to_group")
        ):
            self.relay_mode = "中转到群聊"
            self.forward_to_self = False
            self.forward_relay_group = relay_group_cfg
        elif (
            "自身" in relay_mode_raw
            or "Bot" in relay_mode_raw
            or relay_mode_raw in ("self", "bot", "to_self")
        ):
            self.relay_mode = "中转给Bot自身"
            self.forward_to_self = True
            self.forward_relay_group = ""
        elif relay_group_cfg:
            # 未选中转方式但填了中转群号 → 按中转到群聊
            self.relay_mode = "中转到群聊"
            self.forward_to_self = False
            self.forward_relay_group = relay_group_cfg
        elif forward.get("forward_to_self", False):
            self.relay_mode = "中转给Bot自身"
            self.forward_to_self = True
            self.forward_relay_group = ""
        else:
            self.relay_mode = "中转给Bot自身"
            self.forward_to_self = True
            self.forward_relay_group = ""
        self.auto_recall_relay = forward.get("auto_recall_relay", True)

        # 默认始终伪造合并转发（聊天记录）恢复
        self.enable_fake_forward = True
        self.context = context
        self.temp_path = Path(StarTools.get_data_dir("astrbot_plugin_revoke_guard"))
        self.temp_path.mkdir(exist_ok=True)
        self.video_cache_path = self.temp_path / "videos"
        self.video_cache_path.mkdir(exist_ok=True)
        self.voice_cache_path = self.temp_path / "voices"
        self.voice_cache_path.mkdir(exist_ok=True)
        self.file_cache_path = self.temp_path / "files"
        self.file_cache_path.mkdir(exist_ok=True)

        self._cached_self_id: str | None = None
        self._cache_tasks: dict[str, CacheTaskState] = {}
        self._cleanup_cache_on_startup()

    async def initialize(self) -> None:
        logger.info(
            f"[{self.instance_id}] 插件已加载。"
            f" 中转方式={self.relay_mode},"
            f" 中转群={self.forward_relay_group or '-'},"
            f" to_self={self.forward_to_self}"
        )

    async def terminate(self) -> None:
        logger.info(f"[{self.instance_id}] 插件已卸载/重载。")

    async def _get_targets_for_group(self, group_id: str) -> List[tuple]:
        """根据插件配置返回通知目标（私聊 / 群聊）。"""
        targets = []
        mode = self.notify_mode
        if mode in ("私聊用户", "私聊用户和发送到群聊"):
            targets.extend(("private", tid) for tid in self.target_receivers)
        if mode in ("发送到群聊", "私聊用户和发送到群聊"):
            targets.extend(("group", tid) for tid in self.target_groups)
        # 未识别的通知位置时兜底：两类都发
        if not targets and mode not in (
            "私聊用户",
            "发送到群聊",
            "私聊用户和发送到群聊",
        ):
            targets = [("private", tid) for tid in self.target_receivers] + [
                ("group", tid) for tid in self.target_groups
            ]
        return targets

    async def _download_video_from_url(self, url: str, save_path: Path) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=120) as response:
                    if response.status == 200:
                        content = await response.read()
                        with open(save_path, "wb") as f:
                            f.write(content)
                        return True
                    else:
                        logger.error(
                            f"[{self.instance_id}] 视频下载失败，HTTP 状态码: {response.status}"
                        )
                        return False
        except Exception as e:
            logger.error(
                f"[{self.instance_id}] 视频下载过程中发生异常: {e}\n{traceback.format_exc()}"
            )
            return False

    def _cleanup_cache_on_startup(self):
        now = time.time()
        expired_count = 0
        for cache_dir in [
            self.video_cache_path,
            self.voice_cache_path,
            self.file_cache_path,
            self.temp_path,
        ]:
            for file in cache_dir.glob("*"):
                if file.is_dir():
                    continue
                try:
                    if now - file.stat().st_mtime > self.cache_expiration_time:
                        file.unlink(missing_ok=True)
                        expired_count += 1
                except Exception:
                    continue
        logger.info(
            f"[{self.instance_id}] 缓存清理完成，移除了 {expired_count} 个过期文件。"
        )

    async def _get_self_id(self, client) -> str | None:
        """获取机器人自身 QQ 号，结果在整个插件生命周期内缓存。"""
        if self._cached_self_id is not None:
            return self._cached_self_id
        try:
            login_info = await client.api.call_action("get_login_info")
            self._cached_self_id = str(login_info.get("user_id"))
        except Exception:
            pass
        return self._cached_self_id

    async def _auto_recall_relay_msg(self, client, relay_msg_id: int):
        """自动撤回中转群的消息"""
        await asyncio.sleep(self.cache_expiration_time)
        try:
            await client.api.call_action("delete_msg", message_id=relay_msg_id)
        except Exception as e:
            logger.error(
                f"[{self.instance_id}] 自动撤回中转群消息失败 (ID: {relay_msg_id}): {e}"
            )

    def _is_monitored(self, group_id: str) -> bool:
        mode = self.monitor_mode
        gid = str(group_id)
        if mode == "白名单群聊":
            return gid in self.whitelist_groups
        if mode == "黑名单群聊":
            return gid not in self.blacklist_groups
        # 所有群聊
        return True

    def _should_skip_cache(self, group_id: str, sender_id: str, message_type) -> bool:
        return (
            message_type != MessageType.GROUP_MESSAGE
            or not self._is_monitored(group_id)
        )

    def _get_cache_file_path(self, group_id: str, message_id: str) -> Path:
        return self.temp_path / f"cache_{group_id}_{message_id}.json"

    def _find_cache_file(self, group_id: str, message_id: str) -> Path | None:
        file_path = self._get_cache_file_path(group_id, message_id)
        if file_path.exists():
            return file_path
        return next(self.temp_path.glob(f"*_{group_id}_{message_id}.json"), None)

    def _write_cache_record(self, file_path: Path, data: dict[str, Any]) -> None:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    async def _get_file_with_retry(self, comp) -> str:
        last_error = None
        for attempt in range(1, self.file_download_retry_attempts + 1):
            try:
                return await comp.get_file()
            except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                last_error = e
                logger.warning(
                    f"[{self.instance_id}] [File处理] 下载文件失败，第 {attempt}/{self.file_download_retry_attempts} 次重试: {e}"
                )
                if attempt >= self.file_download_retry_attempts:
                    break
                await asyncio.sleep(self.file_download_retry_delay)
        if last_error:
            raise last_error
        return ""

    async def _wait_for_cache_task_result(
        self, cache_key: str, timeout: float | None = None
    ) -> dict[str, Any] | None:
        state = self._cache_tasks.get(cache_key)
        if not state:
            return None
        if not state.done.is_set():
            try:
                await asyncio.wait_for(state.done.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"[{self.instance_id}] 缓存任务仍在进行中: {cache_key}")
                return None
        return state.result

    async def _expire_cache_task_state(self, cache_key: str, state: CacheTaskState):
        await asyncio.sleep(self.cache_expiration_time)
        current_state = self._cache_tasks.get(cache_key)
        if current_state is state:
            self._cache_tasks.pop(cache_key, None)

    async def _run_cache_task(
        self, cache_key: str, state: CacheTaskState, payload: dict[str, Any]
    ):
        file_path = payload.get("file_path")
        try:
            state.result = await self._cache_message_worker(payload)
        except Exception as e:
            state.error = str(e)
            if file_path:
                self._write_cache_record(
                    file_path,
                    {
                        "status": "failed",
                        "group_id": payload.get("group_id"),
                        "message_id": payload.get("message_id"),
                        "sender_id": payload.get("sender_id"),
                        "timestamp": payload.get("timestamp"),
                        "error": str(e),
                    },
                )
                asyncio.create_task(
                    delayed_delete(self.cache_expiration_time, file_path)
                )
            logger.error(
                f"[{self.instance_id}] 缓存任务失败 ({cache_key}): {e}\n{traceback.format_exc()}"
            )
        finally:
            state.task = None
            state.done.set()
            asyncio.create_task(self._expire_cache_task_state(cache_key, state))

    def _schedule_cache_task(
        self, cache_key: str, payload: dict[str, Any]
    ) -> CacheTaskState:
        existing_state = self._cache_tasks.get(cache_key)
        if existing_state and not existing_state.done.is_set():
            return existing_state

        state = CacheTaskState(task=None)
        self._cache_tasks[cache_key] = state
        state.task = asyncio.create_task(
            self._run_cache_task(cache_key, state, payload)
        )
        return state

    def _load_cached_data(self, file_path: Path | None) -> dict[str, Any] | None:
        if not file_path or not file_path.exists():
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"[{self.instance_id}] 读取或解析本地缓存失败: {e}")
            return None

    async def _get_group_context_names(
        self,
        client,
        group_id: str,
        sender_id: str | None = None,
        operator_id: str | None = None,
    ) -> tuple[str, str, str]:
        """统一获取群名、发送者昵称、操作者昵称，失败时自动降级到原始ID。"""
        group_name = str(group_id)
        sender_name = str(sender_id) if sender_id else "未知发送者"
        operator_name = str(operator_id) if operator_id else ""

        group_id_int: int | None
        try:
            group_id_int = int(group_id)
        except Exception:
            group_id_int = None

        if group_id_int is not None:
            try:
                group_info = await client.api.call_action(
                    "get_group_info", group_id=group_id_int
                )
                if isinstance(group_info, dict):
                    group_name = group_info.get("group_name", group_name)
            except Exception:
                pass

        async def _get_member_name(user_id: str | None, default_name: str) -> str:
            if not user_id or group_id_int is None:
                return default_name
            try:
                user_id_int = int(user_id)
            except Exception:
                return default_name
            try:
                member_info = await client.api.call_action(
                    "get_group_member_info",
                    group_id=group_id_int,
                    user_id=user_id_int,
                )
                if isinstance(member_info, dict):
                    card, nickname = (
                        member_info.get("card", ""),
                        member_info.get("nickname", ""),
                    )
                    return card or nickname or default_name
            except Exception:
                pass
            return default_name

        sender_name = await _get_member_name(sender_id, sender_name)
        operator_name = await _get_member_name(operator_id, operator_name)
        return group_name, sender_name, operator_name

    async def _poll_cache_record(
        self,
        group_id: str,
        message_id: str,
    ) -> dict[str, Any] | None:
        short_poll_interval = 0.1
        long_poll_interval = 1.0
        file_path = self._get_cache_file_path(group_id, message_id)
        cached_data = None
        started_at = time.monotonic()
        pending_deadline = time.monotonic() + self.cache_pending_timeout
        logger.debug(
            f"[{self.instance_id}] 开始轮询缓存（群: {group_id}, 消息ID: {message_id}）"
        )

        while time.monotonic() < pending_deadline:
            cached_data = self._load_cached_data(file_path)
            if cached_data and str(cached_data.get("message_id", "")) == message_id:
                break
            await asyncio.sleep(short_poll_interval)
        else:
            return None

        if not cached_data:
            elapsed = time.monotonic() - started_at
            logger.warning(
                f"[{self.instance_id}] 缓存文件读取为空（ID: {message_id}, 耗时: {elapsed:.2f}s）"
            )
            return None

        wait_deadline = started_at + self.cache_expiration_time
        while time.monotonic() < wait_deadline:
            cached_data = self._load_cached_data(file_path)
            if not cached_data:
                await asyncio.sleep(long_poll_interval)
                continue
            status = cached_data.get("status")
            if status in ("done", "failed"):
                elapsed = time.monotonic() - started_at
                logger.debug(
                    f"[{self.instance_id}] 轮询结束（ID: {message_id}, 状态: {status}, 耗时: {elapsed:.2f}s）"
                )
                return cached_data
            await asyncio.sleep(long_poll_interval)

        elapsed = time.monotonic() - started_at
        logger.warning(
            f"[{self.instance_id}] 长轮询超时（ID: {message_id}, 上限: {self.cache_expiration_time}s, 耗时: {elapsed:.2f}s）"
        )
        return None

    def _extract_plain_text(self, components: list) -> str:
        """提取消息中的纯文本内容，用于关键词匹配。"""
        if not components:
            return ""
        parts: list[str] = []
        for comp in components:
            comp_type_name = getattr(comp.type, "name", "unknown")
            if comp_type_name in {"Plain", "Text"}:
                text = str(getattr(comp, "text", "") or "")
                if text:
                    parts.append(text)
            elif comp_type_name == "At":
                qq = getattr(comp, "qq", "")
                name = getattr(comp, "name", "") or qq
                if name:
                    parts.append(f"@{name}")
        return "".join(parts)

    def _contains_ignore_keyword(self, components: list) -> bool:
        """检查消息文本是否包含任一忽略关键词。"""
        if not self.ignore_keywords:
            return False
        text = self._extract_plain_text(components)
        if not text:
            return False
        for keyword in self.ignore_keywords:
            if keyword and keyword in text:
                return True
        return False

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=20)
    async def handle_message_cache(self, event: AstrMessageEvent):
        """处理消息缓存，后台执行缓存任务"""
        group_id = str(event.get_group_id())
        message_id = str(event.message_obj.message_id)
        sender_id = str(event.get_sender_id())
        if self._should_skip_cache(group_id, sender_id, event.get_message_type()):
            return None

        message_obj = event.get_messages()
        components = (
            message_obj.components
            if isinstance(message_obj, MessageChain)
            else message_obj
            if isinstance(message_obj, list)
            else []
        )
        if not components:
            return None
        if self._contains_ignore_keyword(components):
            logger.debug(
                f"[{self.instance_id}] 消息包含忽略关键词，跳过缓存 (群: {group_id}, 发送者: {sender_id})"
            )
            return None
        component_types = [getattr(comp.type, "name", "unknown") for comp in components]

        cache_key = build_cache_task_key(group_id, message_id)
        cache_file = self._get_cache_file_path(group_id, message_id)
        if cache_file and cache_file.exists():
            return None

        existing_state = self._cache_tasks.get(cache_key)
        if existing_state and not existing_state.done.is_set():
            return None

        self._write_cache_record(
            cache_file,
            {
                "status": "pending",
                "group_id": group_id,
                "message_id": message_id,
                "sender_id": sender_id,
                "timestamp": event.message_obj.timestamp,
                "component_types": component_types,
            },
        )

        payload = {
            "group_id": group_id,
            "message_id": message_id,
            "sender_id": sender_id,
            "timestamp": event.message_obj.timestamp,
            "raw_message": event.message_obj.raw_message,
            "components": components,
            "client": event.bot,
            "file_path": cache_file,
        }
        self._schedule_cache_task(cache_key, payload)
        return None

    async def _cache_message_worker(
        self, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        group_id = payload["group_id"]
        message_id = payload["message_id"]
        sender_id = payload["sender_id"]
        timestamp = payload["timestamp"]
        cache_file = payload.get("file_path")
        raw_message = payload.get("raw_message")
        if not isinstance(raw_message, dict):
            raw_message = {}
        components = payload.get("components", [])
        client = payload["client"]

        relay_info = None
        message_list = raw_message.get("message", [])
        is_forward = False
        if isinstance(message_list, list) and message_list:
            first_segment = message_list[0]
            if (
                isinstance(first_segment, dict)
                and first_segment.get("type") == "forward"
            ):
                is_forward = True

        if is_forward and self.forward_to_self:
            for attempt in range(2):
                try:
                    self_id = await self._get_self_id(client)
                    if not self_id:
                        break
                    ret = await client.api.call_action(
                        "forward_friend_single_msg",
                        user_id=int(self_id),
                        message_id=message_id,
                    )

                    relay_msg_id = None
                    relay_msg_time = None
                    if isinstance(ret, dict):
                        _ret_data = ret.get("data", {}) or {}
                        relay_msg_id = (
                            ret.get("message_id") or _ret_data.get("message_id") or ""
                        )
                    if not relay_msg_id:
                        await asyncio.sleep(1)
                        try:
                            history_result = await client.api.call_action(
                                "get_friend_msg_history",
                                user_id=int(self_id),
                                count=10,
                            )
                            messages = []
                            if isinstance(history_result, dict):
                                messages = history_result.get(
                                    "messages", []
                                ) or history_result.get("data", {}).get("messages", [])
                            for msg in reversed(messages):
                                msg_time = int(msg.get("time", 0))
                                if abs(msg_time - timestamp) <= 2:
                                    relay_msg_id = msg.get("message_id")
                                    break
                        except Exception as exc:
                            logger.warning(
                                f"[{self.instance_id}] 查询自身私聊历史消息失败: {exc}"
                            )

                    if relay_msg_id:
                        relay_info = {
                            "relay_msg_id": relay_msg_id,
                            "sender_id": sender_id,
                            "timestamp": timestamp,
                            "group_id": group_id,
                            "is_private_relay": True,
                        }
                        break
                except Exception as exc:
                    logger.warning(
                        f"[{self.instance_id}] ❌ 转发合并消息给机器人自身失败 (attempt {attempt + 1}/2): {exc}"
                    )
                    if attempt == 0:
                        self._cached_self_id = None
                    else:
                        break

        if not relay_info and is_forward and self.forward_relay_group:
            try:
                ret = await client.api.call_action(
                    "forward_group_single_msg",
                    group_id=int(self.forward_relay_group),
                    message_id=message_id,
                )

                relay_msg_id = None
                relay_msg_time = None
                if isinstance(ret, dict):
                    _ret_data = ret.get("data", {}) or {}
                    relay_msg_id = (
                        ret.get("message_id") or _ret_data.get("message_id") or ""
                    )
                if not relay_msg_id:
                    for attempt in range(2):
                        await asyncio.sleep(1)
                        try:
                            self_id = await self._get_self_id(client)

                            found_msg = None
                            next_seq = 0
                            for _ in range(5):
                                history_result = await client.api.call_action(
                                    "get_group_msg_history",
                                    group_id=int(self.forward_relay_group),
                                    message_seq=next_seq,
                                    count=20,
                                )
                                messages = []
                                if isinstance(history_result, dict):
                                    messages = history_result.get("data", {}).get(
                                        "messages", []
                                    )
                                    if not messages:
                                        messages = history_result.get("messages", [])
                                if not messages:
                                    await asyncio.sleep(1)
                                    continue

                                for msg in reversed(messages):
                                    msg_sender_id = str(
                                        msg.get("sender", {}).get("user_id", "")
                                    ) or str(msg.get("user_id", ""))
                                    if self_id and msg_sender_id != self_id:
                                        continue
                                    msg_time = int(msg.get("time", 0))
                                    if abs(msg_time - timestamp) <= 1:
                                        found_msg = msg
                                        break

                                if found_msg:
                                    break

                                oldest_msg = messages[0]
                                next_seq = oldest_msg.get("message_seq")
                                oldest_time = int(oldest_msg.get("time", 0))
                                if timestamp - oldest_time > self.cache_expiration_time:
                                    break
                                if next_seq == 0:
                                    break

                            if found_msg:
                                relay_msg_id = found_msg.get("message_id")
                                relay_msg_time = found_msg.get("time")
                                break
                        except Exception as exc:
                            logger.warning(
                                f"[{self.instance_id}] 查询历史消息失败 (attempt {attempt + 1}/2): {exc}"
                            )

                        if attempt == 0:
                            self._cached_self_id = None

                if relay_msg_id:
                    relay_info = {
                        "relay_msg_id": relay_msg_id,
                        "sender_id": sender_id,
                        "timestamp": timestamp,
                        "relay_timestamp": relay_msg_time,
                        "group_id": group_id,
                    }
                    if self.auto_recall_relay:
                        asyncio.create_task(
                            self._auto_recall_relay_msg(client, relay_msg_id)
                        )
            except Exception as exc:
                logger.error(
                    f"[{self.instance_id}] ❌ 转发合并消息到中转群失败: {exc}\n{traceback.format_exc()}"
                )

        timestamp_ms = int(time.time() * 1000)
        raw_file_names = []
        raw_file_sizes = {}
        raw_video_sizes = {}
        raw_record_urls = {}
        try:
            if isinstance(message_list, list):
                for segment in message_list:
                    if not isinstance(segment, dict):
                        continue
                    if segment.get("type") == "file":
                        file_name = segment.get("data", {}).get("file")
                        file_size = segment.get("data", {}).get("file_size")
                        if file_name:
                            raw_file_names.append(file_name)
                        if file_size:
                            try:
                                raw_file_sizes[file_name] = (
                                    int(file_size)
                                    if isinstance(file_size, str)
                                    else file_size
                                )
                            except ValueError:
                                logger.warning(
                                    f"[RevokeGuard] 无法解析文件大小: {file_size}"
                                )
                    elif segment.get("type") == "video":
                        file_id = segment.get("data", {}).get("file")
                        file_size = segment.get("data", {}).get("file_size")
                        if file_id and file_size:
                            try:
                                raw_video_sizes[file_id] = (
                                    int(file_size)
                                    if isinstance(file_size, str)
                                    else file_size
                                )
                            except ValueError:
                                logger.warning(
                                    f"[RevokeGuard] 无法解析视频大小: {file_size}"
                                )
                    elif segment.get("type") == "record":
                        file_id = segment.get("data", {}).get("file")
                        url = segment.get("data", {}).get("url")
                        if file_id and url:
                            raw_record_urls[file_id] = url
        except Exception as exc:
            logger.warning(f"[RevokeGuard] 解析 raw_message 失败: {exc}")

        local_file_map = {}
        original_image_urls = _build_original_image_urls(raw_message)
        original_image_idx = -1
        has_downloadable_content = any(
            getattr(comp.type, "name", "") in ["Image", "Video", "Record", "File"]
            for comp in components
        )
        image_cache: dict[str, str] = {}
        file_cache: dict[str, str] = {}
        if has_downloadable_content:
            for comp in components:
                comp_type_name = getattr(comp.type, "name", "unknown")

                if comp_type_name == "Video":
                    file_id = getattr(comp, "file", None)
                    if not file_id:
                        continue

                    video_size = raw_video_sizes.get(file_id)
                    if video_size and self.file_size_threshold_mb > 0:
                        video_size_mb = video_size / (1024 * 1024)
                        if video_size_mb > self.file_size_threshold_mb:
                            file_cache[file_id] = (
                                f"[视频过大未缓存: {video_size_mb:.2f} MB]"
                            )
                            continue

                    try:
                        ret = await client.api.call_action(
                            "get_file", **{"file_id": file_id}
                        )
                        download_url = ret.get("url")
                        if not download_url:
                            file_cache[file_id] = "Error: API did not return a URL."
                            continue

                        file_size_from_api = ret.get("file_size")
                        if file_size_from_api and self.file_size_threshold_mb > 0:
                            try:
                                file_size_int = (
                                    int(file_size_from_api)
                                    if isinstance(file_size_from_api, str)
                                    else file_size_from_api
                                )
                                api_size_mb = file_size_int / (1024 * 1024)
                                if api_size_mb > self.file_size_threshold_mb:
                                    file_cache[file_id] = (
                                        f"[视频过大未缓存: {api_size_mb:.2f} MB]"
                                    )
                                    continue
                            except (ValueError, TypeError):
                                logger.warning(
                                    f"[{self.instance_id}] 无法解析API返回的文件大小: {file_size_from_api}"
                                )

                        original_filename = getattr(
                            comp, "name", file_id.split("/")[-1]
                        )
                        if not original_filename or len(original_filename) < 5:
                            original_filename = f"{timestamp_ms}.mp4"

                        dest_path = (
                            self.video_cache_path
                            / f"{timestamp_ms}_{original_filename}"
                        )
                        if await self._download_video_from_url(download_url, dest_path):
                            file_cache[file_id] = str(dest_path.absolute())
                            asyncio.create_task(
                                delayed_delete(self.cache_expiration_time, dest_path)
                            )
                        else:
                            file_cache[file_id] = (
                                f"Error: Download failed from {download_url}"
                            )
                    except Exception as exc:
                        logger.error(
                            f"[{self.instance_id}] ❌ 处理视频缓存时发生错误: {exc}\n{traceback.format_exc()}"
                        )
                        file_cache[file_id] = "Error: Exception during cache process."

                elif comp_type_name == "Record":
                    file_id = getattr(comp, "file", None)
                    if not file_id:
                        continue

                    try:
                        record_url = getattr(comp, "url", None) or raw_record_urls.get(
                            file_id
                        )
                        if record_url:
                            try:
                                original_suffix = ".amr"
                                if getattr(comp, "file", "").endswith(".slk"):
                                    original_suffix = ".slk"
                                permanent_path = (
                                    self.voice_cache_path
                                    / f"{timestamp_ms}{original_suffix}"
                                )
                                headers = {"User-Agent": "Mozilla/5.0 ..."}
                                async with aiohttp.ClientSession() as session:
                                    async with session.get(
                                        record_url, headers=headers, timeout=15
                                    ) as response:
                                        response.raise_for_status()
                                        voice_bytes = await response.read()
                                        with open(permanent_path, "wb") as f:
                                            f.write(voice_bytes)
                                os.chmod(permanent_path, 0o644)
                                file_cache[file_id] = str(permanent_path.absolute())
                                asyncio.create_task(
                                    delayed_delete(
                                        self.cache_expiration_time, permanent_path
                                    )
                                )
                                continue
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] [Record处理] 尝试通过 URL 下载失败: {exc}，将尝试使用本地路径兜底。"
                                )

                        ret = await client.api.call_action(
                            "get_file", **{"file_id": file_id}
                        )
                        local_path = ret.get("file")
                        if not local_path or not os.path.exists(local_path):
                            file_cache[file_id] = (
                                "Error: API did not return a valid file path."
                            )
                            continue

                        original_suffix = Path(local_path).suffix or ".amr"
                        permanent_path = (
                            self.voice_cache_path / f"{timestamp_ms}{original_suffix}"
                        )
                        shutil.copy(local_path, permanent_path)
                        os.chmod(permanent_path, 0o644)
                        file_cache[file_id] = str(permanent_path.absolute())
                        asyncio.create_task(
                            delayed_delete(self.cache_expiration_time, permanent_path)
                        )
                    except Exception as exc:
                        logger.error(
                            f"[{self.instance_id}] ❌ 处理 Record 缓存时发生错误: {exc}\n{traceback.format_exc()}"
                        )
                        file_cache[file_id] = "Error: Exception during cache process."

                elif comp_type_name == "File":
                    try:
                        original_filename = (
                            raw_file_names[0] if raw_file_names else None
                        )
                        file_size = (
                            raw_file_sizes.get(original_filename)
                            if original_filename
                            else None
                        )
                        if file_size and self.file_size_threshold_mb > 0:
                            file_size_mb = file_size / (1024 * 1024)
                            if file_size_mb > self.file_size_threshold_mb:
                                unique_key = getattr(comp, "url", None)
                                if unique_key:
                                    local_file_map[unique_key] = (
                                        f"[文件过大未缓存: {file_size_mb:.2f} MB, 文件名: {original_filename}]"
                                    )
                                if raw_file_names:
                                    raw_file_names.pop(0)
                                continue

                        temp_file_path = await self._get_file_with_retry(comp)
                        if not temp_file_path or not os.path.exists(temp_file_path):
                            continue

                        if not original_filename and raw_file_names:
                            original_filename = raw_file_names.pop(0)
                        if not original_filename:
                            original_filename = getattr(
                                comp, "name", Path(temp_file_path).name
                            )
                        if (
                            not original_filename
                            or original_filename == Path(temp_file_path).name
                        ):
                            original_filename = f"未知文件_{timestamp_ms}.dat"

                        permanent_path = (
                            self.file_cache_path / f"{timestamp_ms}_{original_filename}"
                        )
                        shutil.copy(temp_file_path, permanent_path)

                        unique_key = getattr(comp, "url", None)
                        if unique_key:
                            local_file_map[unique_key] = str(permanent_path)
                            asyncio.create_task(
                                delayed_delete(
                                    self.cache_expiration_time, permanent_path
                                )
                            )
                    except Exception as exc:
                        logger.error(
                            f"[{self.instance_id}] ❌ 处理 File 缓存时发生错误: {exc}\n{traceback.format_exc()}"
                        )

                elif comp_type_name == "Image":
                    image_url = getattr(comp, "url", None)
                    image_file = (
                        getattr(comp, "file", None)
                        or getattr(comp, "file_", None)
                        or getattr(comp, "filename", None)
                    )
                    if not image_url and not image_file:
                        continue

                    original_image_idx += 1
                    orig_url = (
                        original_image_urls[original_image_idx]
                        if original_image_idx < len(original_image_urls)
                        else None
                    )
                    # 序列化时用 url 做 key；没有 url 时用 file 兜底
                    original_url = image_url or str(image_file)

                    cached_path = None
                    candidates = []
                    for u in (orig_url, image_url, image_file):
                        if u and str(u) not in candidates:
                            candidates.append(str(u))

                    # 1) 本地路径直接复制（AstrBot/LLBot 可能已落盘）
                    for cand in candidates:
                        if cand.startswith(("http://", "https://")):
                            continue
                        # 去掉 file:// 前缀
                        local_cand = cand
                        if local_cand.startswith("file:///"):
                            local_cand = local_cand[8:]
                        elif local_cand.startswith("file://"):
                            local_cand = local_cand[7:]
                        src = Path(local_cand)
                        if src.exists() and src.is_file():
                            try:
                                dest_name = (
                                    f"image_{int(time.time() * 1000)}"
                                    f"{src.suffix or '.jpg'}"
                                )
                                dest = self.temp_path / dest_name
                                shutil.copy2(str(src), str(dest))
                                cached_path = str(dest.absolute())
                                asyncio.create_task(
                                    delayed_delete(self.cache_expiration_time, dest)
                                )
                                break
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] 图片本地复制失败 ({cand}): {exc}"
                                )

                    # 2) HTTP 下载
                    if not cached_path:
                        headers = {
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/120.0.0.0 Safari/537.36"
                            ),
                            "Referer": "https://qzone.qq.com/",
                        }
                        for cand in candidates:
                            if not cand.startswith(("http://", "https://")):
                                continue
                            try:
                                dest_name = (
                                    f"image_{int(time.time() * 1000)}"
                                    f"{Path(cand.split('?', 1)[0]).suffix or '.jpg'}"
                                )
                                dest = self.temp_path / dest_name
                                async with aiohttp.ClientSession() as session:
                                    async with session.get(
                                        cand, headers=headers, timeout=20
                                    ) as response:
                                        response.raise_for_status()
                                        image_bytes = await response.read()
                                        if len(image_bytes) < 100:
                                            raise ValueError("图片内容过小，可能无效")
                                        with open(dest, "wb") as f:
                                            f.write(image_bytes)
                                cached_path = str(dest.absolute())
                                asyncio.create_task(
                                    delayed_delete(self.cache_expiration_time, dest)
                                )
                                break
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] 图片HTTP下载失败 ({cand[:80]}): {exc}"
                                )

                    # 3) NapCat/go-cq get_image 兜底（秒撤后 URL 可能失效，本地缓存仍在）
                    if not cached_path and image_file:
                        for key in ("file", "file_id"):
                            try:
                                ret = await client.api.call_action(
                                    "get_image", **{key: str(image_file)}
                                )
                                if not isinstance(ret, dict):
                                    continue
                                data = ret.get("data", ret) or {}
                                local_file = (
                                    data.get("file")
                                    or data.get("path")
                                    or data.get("filename")
                                )
                                file_url = data.get("url")
                                if local_file:
                                    src = Path(str(local_file))
                                    if src.exists():
                                        dest_name = (
                                            f"image_{int(time.time() * 1000)}"
                                            f"{src.suffix or '.jpg'}"
                                        )
                                        dest = self.temp_path / dest_name
                                        shutil.copy2(str(src), str(dest))
                                        cached_path = str(dest.absolute())
                                        asyncio.create_task(
                                            delayed_delete(
                                                self.cache_expiration_time, dest
                                            )
                                        )
                                        break
                                if file_url and str(file_url).startswith(
                                    ("http://", "https://")
                                ):
                                    dest_name = (
                                        f"image_{int(time.time() * 1000)}.jpg"
                                    )
                                    dest = self.temp_path / dest_name
                                    headers = {
                                        "User-Agent": "Mozilla/5.0",
                                        "Referer": "https://qzone.qq.com/",
                                    }
                                    async with aiohttp.ClientSession() as session:
                                        async with session.get(
                                            str(file_url), headers=headers, timeout=20
                                        ) as response:
                                            response.raise_for_status()
                                            image_bytes = await response.read()
                                            with open(dest, "wb") as f:
                                                f.write(image_bytes)
                                    cached_path = str(dest.absolute())
                                    asyncio.create_task(
                                        delayed_delete(
                                            self.cache_expiration_time, dest
                                        )
                                    )
                                    break
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] get_image({key})失败: {exc}"
                                )
                        if cached_path:
                            pass  # already set

                    if cached_path:
                        image_cache[original_url] = cached_path
                        logger.info(
                            f"[{self.instance_id}] 图片已缓存: {original_url[:60]} -> {cached_path}"
                        )
                    else:
                        logger.error(
                            f"[{self.instance_id}] ❌ 图片缓存全部失败 "
                            f"(url={image_url}, file={image_file})"
                        )


        file_path = (
            cache_file
            if isinstance(cache_file, Path)
            else self._get_cache_file_path(group_id, message_id)
        )
        serialized_components = _serialize_components(components)
        for comp_dict in serialized_components:
            ctype = comp_dict.get("type")
            if ctype == "Image":
                url = comp_dict.get("url")
                if url in image_cache:
                    comp_dict["url"] = image_cache[url]
            elif ctype in ("Video", "Record"):
                file_val = comp_dict.get("file")
                if file_val in file_cache:
                    comp_dict["file"] = file_cache[file_val]
        self._write_cache_record(
            file_path,
            {
                "status": "done",
                "group_id": group_id,
                "message_id": message_id,
                "sender_id": sender_id,
                "timestamp": timestamp,
                "components": serialized_components,
                "local_file_map": local_file_map,
                "relay_info": relay_info,
            },
        )

        asyncio.create_task(delayed_delete(self.cache_expiration_time, file_path))
        return {"file_path": str(file_path), "relay_info": relay_info}

    def _describe_message_type(
        self, components: list | None = None, *, is_forward: bool = False
    ) -> str:
        """根据组件类型生成人类可读的消息类型描述。"""
        if is_forward:
            return "合并转发记录"

        if not components:
            return "未知消息"

        type_names: list[str] = []
        for comp in components:
            name = getattr(getattr(comp, "type", None), "name", None)
            if not name and isinstance(comp, dict):
                name = comp.get("type")
            if name:
                type_names.append(str(name))

        if not type_names:
            return "未知消息"

        unique = set(type_names)
        # 纯合并转发
        if unique == {"Forward"} or (len(unique) == 1 and "Forward" in unique):
            return "合并转发记录"

        has_text = bool(unique & {"Plain", "Text"})
        has_image = "Image" in unique
        has_video = "Video" in unique
        has_record = "Record" in unique
        has_file = "File" in unique
        has_json = "Json" in unique
        has_face = "Face" in unique
        has_at = "At" in unique
        has_forward = "Forward" in unique
        has_reply = "Reply" in unique

        # 单一主类型优先
        content_flags = [
            has_text or has_face or has_at or has_reply,
            has_image,
            has_video,
            has_record,
            has_file,
            has_json,
            has_forward,
        ]
        content_count = sum(1 for x in content_flags if x)

        if has_forward and content_count == 1:
            return "合并转发记录"
        if has_video and content_count == 1:
            return "视频消息"
        if has_record and content_count == 1:
            return "语音消息"
        if has_file and content_count == 1:
            return "文件消息"
        if has_json and content_count == 1:
            return "小程序/卡片消息"
        if has_image and not (has_text or has_face or has_at) and content_count == 1:
            return "图片消息"
        if (has_text or has_face or has_at or has_reply) and not (
            has_image or has_video or has_record or has_file or has_json or has_forward
        ):
            return "文字消息"
        if has_image and (has_text or has_face or has_at):
            # 图文，可能还夹杂其它
            extras = []
            if has_video:
                extras.append("视频")
            if has_record:
                extras.append("语音")
            if has_file:
                extras.append("文件")
            if has_forward:
                extras.append("合并转发")
            if extras:
                return "混合消息（图文+" + "+".join(extras) + "）"
            return "图文消息"

        # 多种内容混合
        parts = []
        if has_text or has_face or has_at or has_reply:
            parts.append("文字")
        if has_image:
            parts.append("图片")
        if has_video:
            parts.append("视频")
        if has_record:
            parts.append("语音")
        if has_file:
            parts.append("文件")
        if has_json:
            parts.append("小程序")
        if has_forward:
            parts.append("合并转发")
        if not parts:
            return "未知消息"
        if len(parts) == 1:
            mapping = {
                "文字": "文字消息",
                "图片": "图片消息",
                "视频": "视频消息",
                "语音": "语音消息",
                "文件": "文件消息",
                "小程序": "小程序/卡片消息",
                "合并转发": "合并转发记录",
            }
            return mapping.get(parts[0], parts[0] + "消息")
        return "混合消息（" + "+".join(parts) + "）"

    def _create_recall_notification_header(
        self,
        group_name: str,
        group_id: str,
        member_nickname: str,
        sender_id: str,
        operator_nickname: str,
        operator_id: str,
        timestamp: int,
        message_type: str | None = None,
    ) -> str:
        """生成统一的撤回通知消息头"""
        message_time_str = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            if timestamp
            else "未知时间"
        )
        if operator_id == sender_id:
            header = (
                f"【撤回提醒】\n群聊：{group_name} ({group_id})\n"
                f"发送者：{member_nickname} ({sender_id})\n"
                f"时间：{message_time_str}"
            )
        else:
            header = (
                f"【撤回提醒】\n群聊：{group_name} ({group_id})\n"
                f"发送者：{member_nickname} ({sender_id})\n"
                f"操作者：{operator_nickname} ({operator_id})\n"
                f"时间：{message_time_str}"
            )
        if message_type:
            header += f"\n消息类型：{message_type}"
        return header

    async def _send_to_target(
        self, client, target_type: str, target_id_str: str, message
    ):
        """按目标类型发送消息。"""
        if target_type == "private":
            await client.send_private_msg(user_id=int(target_id_str), message=message)
        else:
            await client.send_group_msg(group_id=int(target_id_str), message=message)

    def _message_to_plain_text(self, message) -> str:
        """将 go-cqhttp 消息数组压缩成便于兜底通知的纯文本。"""
        if isinstance(message, str):
            return message.strip()
        if not isinstance(message, list):
            return str(message).strip()

        type_map = {
            "image": "[图片]",
            "face": "[表情]",
            "video": "[视频]",
            "record": "[语音]",
            "file": "[文件]",
            "json": "[小程序]",
            "forward": "[合并转发]",
            "at": "[@]",
        }
        parts = []
        for part in message:
            if not isinstance(part, dict):
                continue
            part_type = part.get("type", "")
            data = part.get("data", {}) or {}
            if part_type == "text":
                text = str(data.get("text", "")).strip()
                if text:
                    parts.append(text)
            else:
                parts.append(type_map.get(part_type, f"[{part_type or '未知内容'}]"))

        merged = "\n".join(part for part in parts if part).strip()
        return merged

    async def _notify_send_failure(
        self,
        client,
        target_type: str,
        target_id_str: str,
        context: str,
        original_error: Exception,
        fallback_text: str,
    ):
        """发送失败后的文本兜底通知，尽量保证目标端能收到异常说明。"""
        original_trace = "".join(
            traceback.format_exception(
                type(original_error), original_error, original_error.__traceback__
            )
        )
        logger.error(
            f"[{self.instance_id}] ❌ {context}失败到 {target_type} {target_id_str}：{original_error}\n{original_trace}"
        )
        try:
            await self._send_to_target(
                client, target_type, target_id_str, fallback_text
            )
            logger.warning(
                f"[{self.instance_id}] ⚠️ {context}失败，已向 {target_type} {target_id_str} 发送文本兜底通知。"
            )
        except Exception as fallback_error:
            fallback_trace = "".join(
                traceback.format_exception(
                    type(fallback_error), fallback_error, fallback_error.__traceback__
                )
            )
            logger.error(
                f"[{self.instance_id}] ❌ {context}的文本兜底通知也失败到 {target_type} {target_id_str}："
                f"{fallback_error}\n{fallback_trace}"
            )

    async def _send_with_text_fallback(
        self,
        client,
        target_type: str,
        target_id_str: str,
        message,
        context: str,
        fallback_text: str,
    ):
        """优先发送原始内容，失败时降级为纯文本提醒。"""
        try:
            await self._send_to_target(client, target_type, target_id_str, message)
            return True
        except Exception as error:
            await self._notify_send_failure(
                client, target_type, target_id_str, context, error, fallback_text
            )
            return False

    async def _send_fake_forward(
        self,
        client,
        target_type: str,
        target_id_str: str,
        sender_id: str,
        member_nickname: str,
        timestamp: int,
        components: list,
        local_file_map: dict,
        local_files_to_cleanup: list,
    ) -> str | None:
        """基于缓存的组件列表构造伪造的合并转发消息（聊天记录样式）。
        成功时返回发送出的消息 ID，失败返回 None。
        """
        try:
            async with aiohttp.ClientSession() as session:
                segments = []
                for comp in components:
                    parts = await _process_component_and_get_gocq_part(
                        comp,
                        session,
                        self.temp_path,
                        local_files_to_cleanup,
                        local_file_map,
                        include_reply=True,
                    )
                    segments.extend(parts)
            if not segments:
                return None
            # NapCat 以 user_id(数值)+nickname 识别节点发言人；同时兼容 uin/name
            sender_uin = str(sender_id or "").strip() or "10000"
            sender_name = str(member_nickname or sender_uin).strip() or sender_uin
            try:
                sender_uin_int = int(sender_uin)
            except (TypeError, ValueError):
                sender_uin_int = 10000
            node = {
                "type": "node",
                "data": {
                    "user_id": sender_uin_int,
                    "nickname": sender_name,
                    "uin": sender_uin_int,
                    "name": sender_name,
                    "time": int(timestamp or 0),
                    "content": segments,
                },
            }
            action = (
                "send_private_forward_msg"
                if target_type == "private"
                else "send_group_forward_msg"
            )
            params = (
                {"user_id": int(target_id_str)}
                if target_type == "private"
                else {"group_id": int(target_id_str)}
            )
            ret = await client.api.call_action(action, messages=[node], **params)
            msg_id = None
            if isinstance(ret, dict):
                _ret_data = ret.get("data", {}) or {}
                msg_id = (
                    ret.get("message_id")
                    or _ret_data.get("message_id")
                    or ""
                )
            return str(msg_id) if msg_id else None
        except Exception as e:
            logger.warning(
                f"[{self.instance_id}] 伪造合并转发发送失败 ({target_type}/{target_id_str}): {e}"
            )
            return None


    async def _forward_single_msg_to_target(
        self,
        client,
        target_type: str,
        target_id_str: str,
        source_msg_ids: list,
    ) -> str | None:
        """将已有消息按 message_id 转发到目标会话。

        策略：
        1. 先尝试 forward_*_single_msg（普通消息最稳）
        2. 失败或结果不可用时，用 get_forward_msg 取出合并转发节点，
           再 send_*_forward_msg 重建发送（聊天记录专用，避免「该消息类型暂不支持查看」）
        """
        seen = set()
        ordered_ids = []
        for src in source_msg_ids:
            src = str(src or "").strip()
            if src and src not in seen:
                seen.add(src)
                ordered_ids.append(src)
        if not ordered_ids:
            return None

        # 聊天记录场景：优先 get_forward_msg + send_*_forward_msg 重建，
        # 避免 forward_*_single_msg 发出「该消息类型暂不支持查看」的占位消息。
        for src in ordered_ids:
            rebuilt = await self._rebuild_forward_to_target(
                client, target_type, target_id_str, src
            )
            if rebuilt:
                return rebuilt

        # 回退：单条转发（部分协议/普通消息仍可用）
        single_action = (
            "forward_friend_single_msg"
            if target_type == "private"
            else "forward_group_single_msg"
        )
        single_params_base = (
            {"user_id": int(target_id_str)}
            if target_type == "private"
            else {"group_id": int(target_id_str)}
        )
        for src in ordered_ids:
            try:
                ret = await client.api.call_action(
                    single_action, message_id=src, **single_params_base
                )
                msg_id = None
                if isinstance(ret, dict):
                    _ret_data = ret.get("data", {}) or {}
                    msg_id = (
                        ret.get("message_id")
                        or _ret_data.get("message_id")
                        or ""
                    )
                logger.info(
                    f"[{self.instance_id}] 单条转发成功 (src={src}) -> "
                    f"{target_type} {target_id_str}"
                )
                return str(msg_id) if msg_id else "ok"
            except Exception as e:
                logger.warning(
                    f"[{self.instance_id}] 单条转发失败 (src={src}) -> "
                    f"{target_type} {target_id_str}: {e}"
                )
        return None

    async def _rebuild_forward_to_target(
        self,
        client,
        target_type: str,
        target_id_str: str,
        source_msg_id: str,
    ) -> str | None:
        """通过 get_forward_msg 取节点，再用 send_*_forward_msg 重建发送聊天记录。"""
        try:
            detail = await client.api.call_action(
                "get_forward_msg", message_id=str(source_msg_id)
            )
        except Exception as e:
            logger.warning(
                f"[{self.instance_id}] get_forward_msg 失败 (src={source_msg_id}): {e}"
            )
            return None

        messages = []
        if isinstance(detail, dict):
            data = detail.get("data", detail) or {}
            messages = data.get("messages") or []
        if not messages:
            logger.warning(
                f"[{self.instance_id}] get_forward_msg 无节点内容 (src={source_msg_id})"
            )
            return None

        nodes = []
        for item in messages:
            if not isinstance(item, dict):
                continue
            # NapCat / go-cqhttp 两种常见结构：
            # 1) {type:node, data:{...}}
            # 2) {sender:{nickname,user_id}, content/message:[...], time:...}
            if item.get("type") == "node":
                node_data = item.get("data") or {}
                # 统一字段名
                uin = (
                    node_data.get("uin")
                    or node_data.get("user_id")
                    or ""
                )
                name = (
                    node_data.get("name")
                    or node_data.get("nickname")
                    or str(uin)
                    or "用户"
                )
                content = (
                    node_data.get("content")
                    or node_data.get("message")
                    or []
                )
                time_val = node_data.get("time")
            else:
                sender = item.get("sender") or {}
                uin = sender.get("user_id") or item.get("user_id") or ""
                name = (
                    sender.get("nickname")
                    or sender.get("card")
                    or str(uin)
                    or "用户"
                )
                content = item.get("content") or item.get("message") or []
                time_val = item.get("time")

            if isinstance(content, str):
                content = [{"type": "text", "data": {"text": content}}]
            if not isinstance(content, list) or not content:
                continue

            node = {
                "type": "node",
                "data": {
                    "name": str(name),
                    "uin": str(uin) if uin else "10000",
                    "content": content,
                },
            }
            if time_val is not None:
                try:
                    node["data"]["time"] = int(time_val)
                except (TypeError, ValueError):
                    pass
            nodes.append(node)

        if not nodes:
            logger.warning(
                f"[{self.instance_id}] 无法从 get_forward_msg 构造节点 (src={source_msg_id})"
            )
            return None

        action = (
            "send_private_forward_msg"
            if target_type == "private"
            else "send_group_forward_msg"
        )
        params = (
            {"user_id": int(target_id_str)}
            if target_type == "private"
            else {"group_id": int(target_id_str)}
        )
        try:
            ret = await client.api.call_action(action, messages=nodes, **params)
            msg_id = None
            if isinstance(ret, dict):
                _ret_data = ret.get("data", {}) or {}
                msg_id = (
                    ret.get("message_id")
                    or _ret_data.get("message_id")
                    or ""
                )
            logger.info(
                f"[{self.instance_id}] 已重建发送聊天记录到 {target_type} {target_id_str} "
                f"(src={source_msg_id}, nodes={len(nodes)})"
            )
            return str(msg_id) if msg_id else "ok"
        except Exception as e:
            logger.warning(
                f"[{self.instance_id}] send_*_forward_msg 重建失败 "
                f"(src={source_msg_id}) -> {target_type} {target_id_str}: {e}"
            )
            return None


    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=10)
    async def handle_recall_event(self, event: AstrMessageEvent):
        """处理群聊撤回事件"""
        raw_message = event.message_obj.raw_message
        post_type = get_value(raw_message, "post_type")
        if (
            post_type == "notice"
            and get_value(raw_message, "notice_type") == "group_recall"
        ):
            group_id = str(get_value(raw_message, "group_id"))
            message_id = str(get_value(raw_message, "message_id"))
            operator_id = str(get_value(raw_message, "operator_id"))
            recall_sender_id = str(get_value(raw_message, "user_id"))
            if not self._is_monitored(group_id) or not message_id:
                return None
            if operator_id in self.ignore_operators:
                logger.debug(
                    f"[{self.instance_id}] 操作者 {operator_id} 在忽略列表中，跳过处理"
                )
                return None

            client = event.bot
            (
                early_group_name,
                early_member_nickname,
                _,
            ) = await self._get_group_context_names(
                client,
                group_id,
                sender_id=recall_sender_id,
                operator_id=None,
            )
            logger.info(
                f"[{self.instance_id}] 发现撤回。群: {early_group_name} ({group_id}), "
                f"发送者: {early_member_nickname} ({recall_sender_id or '未知'})"
            )

            cache_key = build_cache_task_key(group_id, message_id)
            file_path = self._get_cache_file_path(group_id, message_id)
            cached_data = await self._poll_cache_record(group_id, message_id)

            if not cached_data and cache_key in self._cache_tasks:
                task_result = await self._wait_for_cache_task_result(
                    cache_key, timeout=self.cache_expiration_time
                )
                if task_result and task_result.get("file_path"):
                    file_path = Path(task_result["file_path"])
                    cached_data = self._load_cached_data(file_path)

            if not cached_data:
                file_path = self._find_cache_file(group_id, message_id)
                cached_data = self._load_cached_data(file_path)

            if cached_data and cached_data.get("status") == "failed":
                logger.warning(
                    f"[{self.instance_id}] 消息缓存失败，跳过撤回恢复（ID: {message_id}）。"
                )
                return None
            if cached_data and cached_data.get("status") == "pending":
                logger.warning(
                    f"[{self.instance_id}] 消息缓存仍在进行中，跳过撤回恢复（ID: {message_id}）。"
                )
                return None

            # 如果没有找到缓存数据，则无法恢复
            if not cached_data:
                logger.warning(
                    f"[{self.instance_id}] 找不到消息记录 (ID: {message_id})，可能已过期或未缓存。"
                )
                return None

            # 从缓存数据中提取 relay_info，如果存在
            # 原消息本身就是「聊天记录/合并转发」时走此分支：原样转发聊天记录，不再嵌套伪造一层
            relay_info = cached_data.get("relay_info")

            if relay_info:
                logger.info(
                    f"[{self.instance_id}] 检测到合并转发消息被撤回，原消息ID: {message_id}"
                )
                sender_id = relay_info["sender_id"]

                relay_msg_id = str(relay_info.get("relay_msg_id") or "")
                timestamp = relay_info["timestamp"]
                # 优先用中转群里的副本（撤回后仍可读），其次再试原消息 ID
                candidate_ids = []
                for mid in (relay_msg_id, message_id):
                    mid = str(mid or "").strip()
                    if mid and mid not in candidate_ids:
                        candidate_ids.append(mid)

                try:
                    client = event.bot
                    (
                        group_name,
                        member_nickname,
                        operator_nickname,
                    ) = await self._get_group_context_names(
                        client,
                        group_id,
                        sender_id=sender_id,
                        operator_id=operator_id,
                    )

                    logger.info(
                        f"[{self.instance_id}] 合并转发撤回 - 群: {group_name}, 发送者: {member_nickname}, 操作者: {operator_nickname} ({operator_id})"
                    )

                    targets = await self._get_targets_for_group(group_id)

                    for target_type, target_id in targets:
                        target_id_str = str(target_id)
                        header = self._create_recall_notification_header(
                            group_name,
                            group_id,
                            member_nickname,
                            sender_id,
                            operator_nickname,
                            operator_id,
                            timestamp,
                            message_type=self._describe_message_type(is_forward=True),
                        )

                        forwarded_msg_id = await self._forward_single_msg_to_target(
                            client, target_type, target_id_str, candidate_ids
                        )
                        if forwarded_msg_id:
                            logger.info(
                                f"[{self.instance_id}] 已原样转发聊天记录到 {target_type} {target_id_str}"
                            )

                        if not forwarded_msg_id:
                            fallback_text = (
                                f"{header}\n--------------------\n"
                                f"原消息为合并转发（聊天记录），原样转发失败。\n"
                                f"可能原因：平台超时、风控或消息已不可复现。"
                            )
                            try:
                                await self._send_to_target(
                                    client, target_type, target_id_str, fallback_text
                                )
                            except Exception as e:
                                logger.error(
                                    f"[{self.instance_id}] 发送聊天记录失败通知也失败: {e}"
                                )
                            continue

                        # 转发成功后，引用该聊天记录补充撤回信息（与普通消息的新交互一致）
                        await asyncio.sleep(0.3)
                        if forwarded_msg_id and forwarded_msg_id != "ok":
                            reply_message = [
                                {
                                    "type": "reply",
                                    "data": {"id": str(forwarded_msg_id)},
                                },
                                {
                                    "type": "text",
                                    "data": {"text": header},
                                },
                            ]
                            try:
                                await self._send_to_target(
                                    client, target_type, target_id_str, reply_message
                                )
                            except Exception as e:
                                logger.warning(
                                    f"[{self.instance_id}] 引用聊天记录发送撤回信息失败，降级为普通文本: {e}"
                                )
                                try:
                                    await self._send_to_target(
                                        client, target_type, target_id_str, header
                                    )
                                except Exception as e2:
                                    logger.error(
                                        f"[{self.instance_id}] 发送撤回信息也失败: {e2}"
                                    )
                        else:
                            try:
                                await self._send_to_target(
                                    client, target_type, target_id_str, header
                                )
                            except Exception as e:
                                logger.error(
                                    f"[{self.instance_id}] 发送撤回信息失败: {e}"
                                )

                    # 立即撤回中转群的消息 (如果是私聊中转则不撤回)
                    if (
                        self.auto_recall_relay
                        and relay_msg_id
                        and not relay_info.get("is_private_relay")
                    ):
                        try:
                            await client.api.call_action(
                                "delete_msg", message_id=relay_msg_id
                            )
                        except Exception as e:
                            logger.error(
                                f"[{self.instance_id}] 撤回中转群消息失败: {e}"
                            )

                except Exception as e:
                    logger.error(f"[{self.instance_id}] 处理合并转发撤回失败: {e}")

                return None
            if cached_data:
                local_files_to_cleanup = []
                try:
                    sender_id = cached_data["sender_id"]
                    local_file_map = cached_data.get("local_file_map", {})
                    if operator_id in self.ignore_operators:
                        logger.debug(
                            f"[{self.instance_id}] 操作人 {operator_id} 在忽略列表中，跳过处理"
                        )
                        return None

                    cached_components_data = cached_data.get("components", [])

                    unsupported_types = set()
                    supported_types_set = set(COMPONENT_MAP.keys()) | {"Forward"}
                    for comp_dict in cached_components_data:
                        comp_type_name = comp_dict.get("type")
                        if comp_type_name not in supported_types_set:
                            unsupported_types.add(comp_type_name)

                    components = _deserialize_components(cached_components_data)

                    timestamp = cached_data.get("timestamp")
                    client = event.bot
                    (
                        group_name,
                        member_nickname,
                        operator_nickname,
                    ) = await self._get_group_context_names(
                        client,
                        group_id,
                        sender_id=sender_id,
                        operator_id=operator_id,
                    )

                    special_components = [
                        comp
                        for comp in components
                        if getattr(comp.type, "name", "unknown")
                        in ["Video", "Record", "Json", "File", "Forward"]
                    ]
                    other_components = [
                        comp
                        for comp in components
                        if getattr(comp.type, "name", "unknown")
                        not in ["Video", "Record", "Json", "File", "Forward"]
                    ]

                    # 纯聊天记录消息（无中转缓存时）：直接原样转发，避免伪造嵌套一层
                    pure_forward = (
                        len(special_components) == 1
                        and not other_components
                        and getattr(special_components[0].type, "name", "") == "Forward"
                    )
                    if pure_forward:
                        targets = await self._get_targets_for_group(group_id)
                        notification_prefix = self._create_recall_notification_header(
                            group_name,
                            group_id,
                            member_nickname,
                            sender_id,
                            operator_nickname,
                            operator_id,
                            timestamp,
                            message_type=self._describe_message_type(is_forward=True),
                        )
                        for target_type, target_id in targets:
                            target_id_str = str(target_id)
                            forwarded_msg_id = await self._forward_single_msg_to_target(
                                client,
                                target_type,
                                target_id_str,
                                [message_id],
                            )
                            if not forwarded_msg_id:
                                fallback_text = (
                                    f"{notification_prefix}\n--------------------\n"
                                    f"原消息为合并转发（聊天记录），原样转发失败。"
                                )
                                try:
                                    await self._send_to_target(
                                        client, target_type, target_id_str, fallback_text
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"[{self.instance_id}] 纯聊天记录转发失败通知也失败: {e}"
                                    )
                                continue
                            await asyncio.sleep(0.3)
                            header = notification_prefix
                            if forwarded_msg_id and forwarded_msg_id != "ok":
                                reply_message = [
                                    {
                                        "type": "reply",
                                        "data": {"id": str(forwarded_msg_id)},
                                    },
                                    {"type": "text", "data": {"text": header}},
                                ]
                                try:
                                    await self._send_to_target(
                                        client, target_type, target_id_str, reply_message
                                    )
                                except Exception:
                                    try:
                                        await self._send_to_target(
                                            client, target_type, target_id_str, header
                                        )
                                    except Exception as e:
                                        logger.error(
                                            f"[{self.instance_id}] 发送撤回信息失败: {e}"
                                        )
                            else:
                                try:
                                    await self._send_to_target(
                                        client, target_type, target_id_str, header
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"[{self.instance_id}] 发送撤回信息失败: {e}"
                                    )
                        return None

                    async with aiohttp.ClientSession() as session:
                        targets = await self._get_targets_for_group(group_id)
                        for target_type, target_id in targets:
                            target_id_str = str(target_id)

                            notification_prefix = (
                                self._create_recall_notification_header(
                                    group_name,
                                    group_id,
                                    member_nickname,
                                    sender_id,
                                    operator_nickname,
                                    operator_id,
                                    timestamp,
                                    message_type=self._describe_message_type(
                                        components
                                    ),
                                )
                            )
                            warning_text = (
                                f"\n⚠️ 注意：包含不支持的组件：{', '.join(unsupported_types)}"
                                if unsupported_types
                                else ""
                            )

                            fake_succeeded = False
                            if self.enable_fake_forward:
                                # 开启伪造合并转发时：优先只发聊天记录，再引用补充撤回信息
                                fake_msg_id = await self._send_fake_forward(
                                    client,
                                    target_type,
                                    target_id_str,
                                    sender_id,
                                    member_nickname,
                                    timestamp,
                                    components,
                                    local_file_map,
                                    local_files_to_cleanup,
                                )
                                header_text = f"{notification_prefix}{warning_text}"
                                if fake_msg_id:
                                    fake_succeeded = True
                                    reply_message = [
                                        {
                                            "type": "reply",
                                            "data": {"id": str(fake_msg_id)},
                                        },
                                        {
                                            "type": "text",
                                            "data": {"text": header_text},
                                        },
                                    ]
                                    try:
                                        await self._send_to_target(
                                            client,
                                            target_type,
                                            target_id_str,
                                            reply_message,
                                        )
                                    except Exception as e:
                                        logger.warning(
                                            f"[{self.instance_id}] 引用聊天记录发送撤回信息失败，降级为普通文本: {e}"
                                        )
                                        await self._send_with_text_fallback(
                                            client,
                                            target_type,
                                            target_id_str,
                                            header_text,
                                            "发送撤回信息(引用失败后降级)",
                                            header_text,
                                        )
                                else:
                                    logger.warning(
                                        f"[{self.instance_id}] 伪造合并转发失败，降级为直接发送原消息内容 "
                                        f"-> {target_type} {target_id_str}"
                                    )

                            if not fake_succeeded:
                                # 伪造失败降级：
                                # - 语音/小程序/视频/文件：不单条转发原消息（易变成 Bot 视角的中转记录），直接组件直发
                                # - 纯图/文本等：可尝试按原消息 ID 单条转发（平台本地常仍有内容）
                                single_forwarded = False
                                special_type_names = {
                                    getattr(c.type, "name", "")
                                    for c in components
                                }
                                prefer_direct = bool(
                                    special_type_names
                                    & {"Json", "Record", "Video", "File", "Forward"}
                                )
                                if not prefer_direct:
                                    try:
                                        sf_id = await self._forward_single_msg_to_target(
                                            client,
                                            target_type,
                                            target_id_str,
                                            [message_id],
                                        )
                                        if sf_id:
                                            single_forwarded = True
                                            header_text = (
                                                f"{notification_prefix}{warning_text}"
                                            )
                                            await asyncio.sleep(0.3)
                                            if sf_id != "ok":
                                                reply_message = [
                                                    {
                                                        "type": "reply",
                                                        "data": {"id": str(sf_id)},
                                                    },
                                                    {
                                                        "type": "text",
                                                        "data": {"text": header_text},
                                                    },
                                                ]
                                                try:
                                                    await self._send_to_target(
                                                        client,
                                                        target_type,
                                                        target_id_str,
                                                        reply_message,
                                                    )
                                                except Exception:
                                                    await self._send_to_target(
                                                        client,
                                                        target_type,
                                                        target_id_str,
                                                        header_text,
                                                    )
                                            else:
                                                await self._send_to_target(
                                                    client,
                                                    target_type,
                                                    target_id_str,
                                                    header_text,
                                                )
                                    except Exception as e:
                                        logger.warning(
                                            f"[{self.instance_id}] 原消息单条转发降级失败: {e}"
                                        )

                                if single_forwarded:
                                    continue

                                # 组件直发（伪造失败后的最终回退）
                                if not special_components:
                                    message_parts = []
                                    for comp in other_components:
                                        converted_parts = (
                                            await _process_component_and_get_gocq_part(
                                                comp,
                                                session,
                                                self.temp_path,
                                                local_files_to_cleanup,
                                                local_file_map,
                                                show_qq_in_at=True,
                                            )
                                        )
                                        message_parts.extend(converted_parts)

                                    has_inserted_prefix, final_message_parts = False, []
                                    for part in message_parts:
                                        if not has_inserted_prefix and (
                                            part["type"] in ["text", "image", "face"]
                                        ):
                                            final_message_parts.append(
                                                {
                                                    "type": "text",
                                                    "data": {
                                                        "text": f"{member_nickname}："
                                                    },
                                                }
                                            )
                                            has_inserted_prefix = True
                                            if part["type"] == "text":
                                                final_message_parts[-1]["data"]["text"] += (
                                                    part["data"]["text"]
                                                )
                                                continue
                                        final_message_parts.append(part)

                                    final_prefix_text = f"{notification_prefix}{warning_text}\n--------------------\n"
                                    gocq_content_array = [
                                        {
                                            "type": "text",
                                            "data": {"text": final_prefix_text},
                                        }
                                    ]
                                    gocq_content_array.extend(final_message_parts)

                                    if len(gocq_content_array) > 1 or warning_text:
                                        fallback_text = self._message_to_plain_text(
                                            gocq_content_array
                                        )
                                        if not fallback_text:
                                            fallback_text = f"{notification_prefix}{warning_text}\n--------------------\n[原消息发送失败，且无法提取可展示内容]"
                                        await self._send_with_text_fallback(
                                            client,
                                            target_type,
                                            target_id_str,
                                            gocq_content_array,
                                            "合并消息转发",
                                            fallback_text,
                                        )
                                else:
                                    final_notification_text = f"{notification_prefix}{warning_text}\n--------------------\n内容将分条发送。"
                                    header_ok = await self._send_with_text_fallback(
                                        client,
                                        target_type,
                                        target_id_str,
                                        final_notification_text,
                                        "发送通知头",
                                        f"{notification_prefix}\n[原始通知头发送失败，已切换为简化文本通知]",
                                    )
                                    if not header_ok:
                                        continue

                                    await asyncio.sleep(0.5)

                                    if other_components:
                                        message_parts = []
                                        for comp in other_components:
                                            converted_parts = (
                                                await _process_component_and_get_gocq_part(
                                                    comp,
                                                    session,
                                                    self.temp_path,
                                                    local_files_to_cleanup,
                                                    local_file_map,
                                                    show_qq_in_at=True,
                                                )
                                            )
                                            message_parts.extend(converted_parts)
                                        if message_parts:
                                            content_message = [
                                                {
                                                    "type": "text",
                                                    "data": {
                                                        "text": f"{member_nickname}："
                                                    },
                                                }
                                            ]
                                            if (
                                                message_parts
                                                and message_parts[0]["type"] == "text"
                                            ):
                                                content_message[0]["data"]["text"] += (
                                                    message_parts[0]["data"]["text"]
                                                )
                                                content_message.extend(message_parts[1:])
                                            else:
                                                content_message.extend(message_parts)
                                            fallback_text = self._message_to_plain_text(
                                                content_message
                                            )
                                            if not fallback_text:
                                                fallback_text = f"{member_nickname}：[非特殊内容发送失败，且无法提取文本]"
                                            await self._send_with_text_fallback(
                                                client,
                                                target_type,
                                                target_id_str,
                                                content_message,
                                                "发送非特殊内容",
                                                fallback_text,
                                            )

                                for comp in special_components:
                                    await asyncio.sleep(0.5)
                                    comp_type_name = getattr(comp.type, "name", "unknown")
                                    content_parts = (
                                        await _process_component_and_get_gocq_part(
                                            comp,
                                            session,
                                            self.temp_path,
                                            local_files_to_cleanup,
                                            local_file_map,
                                            show_qq_in_at=True,
                                        )
                                    )
                                    final_parts_to_send = content_parts
                                    if not other_components:
                                        prefix_part = [
                                            {
                                                "type": "text",
                                                "data": {"text": f"{member_nickname}："},
                                            }
                                        ]
                                        final_parts_to_send = prefix_part + content_parts

                                    type_map = {
                                        "Image": "图片",
                                        "Video": "视频",
                                        "Record": "语音",
                                        "File": "文件",
                                        "Forward": "合并转发",
                                        "Json": "小程序",
                                    }
                                    cn_type = type_map.get(comp_type_name, comp_type_name)
                                    fallback_text = self._message_to_plain_text(
                                        final_parts_to_send
                                    )
                                    if (
                                        not fallback_text
                                        or fallback_text == f"{member_nickname}："
                                    ):
                                        fallback_text = f"{member_nickname}：[发送失败: {cn_type} 消息可能包含无法上传的内容、过大或平台暂时超时]"
                                    await self._send_with_text_fallback(
                                        client,
                                        target_type,
                                        target_id_str,
                                        final_parts_to_send,
                                        f"发送特殊内容 ({comp_type_name})",
                                        fallback_text,
                                    )


                finally:
                    if local_files_to_cleanup:
                        asyncio.create_task(
                            _cleanup_local_files(local_files_to_cleanup)
                        )
                    if file_path:
                        asyncio.create_task(delayed_delete(0, file_path))
            else:
                logger.warning(
                    f"[{self.instance_id}] 找不到消息记录 (ID: {message_id})，可能已过期或未缓存。"
                )
        return None
