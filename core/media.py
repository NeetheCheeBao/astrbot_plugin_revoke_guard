"""媒体文件（图片/视频/语音/文件）的下载与本地缓存。

发送伪造聊天记录时使用的图片下载（``download_and_cache_image``）也在这里，
它只需要一个 ``aiohttp.ClientSession`` 和临时目录，因此保持为普通函数。
"""

import asyncio
import os
import shutil
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

from astrbot.api import logger
from astrbot.api import message_components as Comp

from .utils import delayed_delete, now_ms


def build_original_image_urls(raw_message: dict) -> list[str | None]:
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


async def download_and_cache_image(
    session: aiohttp.ClientSession, component: Comp.Image, temp_path: Path
) -> str:
    """把图片组件落到本地临时目录，返回本地绝对路径；失败返回 None。"""
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


@dataclass
class MediaCacheResult:
    """一次消息媒体缓存的结果。"""

    image_cache: dict[str, str] = field(default_factory=dict)
    """图片原始 url -> 本地路径"""

    file_cache: dict[str, str] = field(default_factory=dict)
    """视频/语音的 file_id -> 本地路径（或错误说明）"""

    local_file_map: dict[str, str] = field(default_factory=dict)
    """文件组件的 url -> 本地路径（或「过大未缓存」说明）"""


class MediaMixin:
    """按组件类型下载并缓存媒体文件。"""

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

    async def _get_file_with_retry(self, comp) -> str:
        """调用组件的 get_file()，对网络类异常做有限重试。"""
        last_error = None
        for attempt in range(1, self.settings.file_download_retry_attempts + 1):
            try:
                return await comp.get_file()
            except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                last_error = e
                logger.warning(
                    f"[{self.instance_id}] [File处理] 下载文件失败，"
                    f"第 {attempt}/{self.settings.file_download_retry_attempts} 次重试: {e}"
                )
                if attempt >= self.settings.file_download_retry_attempts:
                    break
                await asyncio.sleep(self.settings.file_download_retry_delay)
        if last_error:
            raise last_error
        return ""

    async def _cache_message_media(
        self, client, components: list, raw_message: dict
    ) -> MediaCacheResult:
        """下载并缓存消息中的图片/视频/语音/文件。

        raw_message 用于读取协议端原始字段（文件大小、语音 url 等）。
        """
        timestamp_ms = now_ms()
        message_list = raw_message.get("message", [])
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
        original_image_urls = build_original_image_urls(raw_message)
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
                    if video_size and self.settings.file_size_threshold_mb > 0:
                        video_size_mb = video_size / (1024 * 1024)
                        if video_size_mb > self.settings.file_size_threshold_mb:
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
                        if (
                            file_size_from_api
                            and self.settings.file_size_threshold_mb > 0
                        ):
                            try:
                                file_size_int = (
                                    int(file_size_from_api)
                                    if isinstance(file_size_from_api, str)
                                    else file_size_from_api
                                )
                                api_size_mb = file_size_int / (1024 * 1024)
                                if (
                                    api_size_mb
                                    > self.settings.file_size_threshold_mb
                                ):
                                    file_cache[file_id] = (
                                        f"[视频过大未缓存: {api_size_mb:.2f} MB]"
                                    )
                                    continue
                            except (ValueError, TypeError):
                                logger.warning(
                                    f"[{self.instance_id}] 无法解析API返回的文件大小: "
                                    f"{file_size_from_api}"
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
                                delayed_delete(
                                    self.settings.cache_expiration_time, dest_path
                                )
                            )
                        else:
                            file_cache[file_id] = (
                                f"Error: Download failed from {download_url}"
                            )
                    except Exception as exc:
                        logger.error(
                            f"[{self.instance_id}] ❌ 处理视频缓存时发生错误: "
                            f"{exc}\n{traceback.format_exc()}"
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
                                        self.settings.cache_expiration_time,
                                        permanent_path,
                                    )
                                )
                                continue
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] [Record处理] "
                                    f"尝试通过 URL 下载失败: {exc}，将尝试使用本地路径兜底。"
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
                            delayed_delete(
                                self.settings.cache_expiration_time, permanent_path
                            )
                        )
                    except Exception as exc:
                        logger.error(
                            f"[{self.instance_id}] ❌ 处理 Record 缓存时发生错误: "
                            f"{exc}\n{traceback.format_exc()}"
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
                        if (
                            file_size
                            and self.settings.file_size_threshold_mb > 0
                        ):
                            file_size_mb = file_size / (1024 * 1024)
                            if file_size_mb > self.settings.file_size_threshold_mb:
                                unique_key = getattr(comp, "url", None)
                                if unique_key:
                                    local_file_map[unique_key] = (
                                        f"[文件过大未缓存: {file_size_mb:.2f} MB, "
                                        f"文件名: {original_filename}]"
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
                            self.file_cache_path
                            / f"{timestamp_ms}_{original_filename}"
                        )
                        shutil.copy(temp_file_path, permanent_path)

                        unique_key = getattr(comp, "url", None)
                        if unique_key:
                            local_file_map[unique_key] = str(permanent_path)
                            asyncio.create_task(
                                delayed_delete(
                                    self.settings.cache_expiration_time,
                                    permanent_path,
                                )
                            )
                    except Exception as exc:
                        logger.error(
                            f"[{self.instance_id}] ❌ 处理 File 缓存时发生错误: "
                            f"{exc}\n{traceback.format_exc()}"
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
                                    delayed_delete(
                                        self.settings.cache_expiration_time, dest
                                    )
                                )
                                break
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] 图片本地复制失败 "
                                    f"({cand}): {exc}"
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
                                    delayed_delete(
                                        self.settings.cache_expiration_time, dest
                                    )
                                )
                                break
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] 图片HTTP下载失败 "
                                    f"({cand[:80]}): {exc}"
                                )

                    # 3) 协议端 get_image 兜底（秒撤后 URL 可能失效，本地缓存仍在）
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
                                                self.settings.cache_expiration_time,
                                                dest,
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
                                            self.settings.cache_expiration_time, dest
                                        )
                                    )
                                    break
                            except Exception as exc:
                                logger.warning(
                                    f"[{self.instance_id}] get_image({key})失败: {exc}"
                                )

                    if cached_path:
                        image_cache[original_url] = cached_path
                        logger.info(
                            f"[{self.instance_id}] 图片已缓存: "
                            f"{original_url[:60]} -> {cached_path}"
                        )
                    else:
                        logger.error(
                            f"[{self.instance_id}] ❌ 图片缓存全部失败 "
                            f"(url={image_url}, file={image_file})"
                        )

        return MediaCacheResult(
            image_cache=image_cache,
            file_cache=file_cache,
            local_file_map=local_file_map,
        )
