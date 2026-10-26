"""消息组件的序列化/反序列化，以及组件到协议端消息段的转换。"""

import base64
import json
from pathlib import Path
from typing import Dict, List

import aiohttp

from astrbot.api import logger

from .constants import COMPONENT_MAP, FALLBACK_NODE_UIN, GOCQ_PART_PLACEHOLDER
from .media import download_and_cache_image


def serialize_components(components: list) -> List[Dict]:
    """把消息组件转成可 JSON 落盘的字典列表。"""
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


def deserialize_components(comp_dicts: List[Dict]) -> List:
    """把落盘的字典列表还原成消息组件，未知类型会被跳过。"""
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


async def component_to_gocq_parts(
    comp,
    session: aiohttp.ClientSession,
    temp_path: Path,
    local_files_to_cleanup: List[str],
    local_file_map: Dict = None,
    show_qq_in_at: bool = False,
    include_reply: bool = False,
) -> List[Dict]:
    """把一个消息组件转换成 go-cqhttp 消息段列表。"""
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
        local_path = await download_and_cache_image(session, comp, temp_path)
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
                f"[RevokeGuard] ❌ 准备发送视频时失败：缓存的视频文件已丢失，"
                f"路径: {cached_video_path_str}"
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
                f"[RevokeGuard] ❌ 准备发送语音时失败：缓存的语音文件已丢失，"
                f"路径: {cached_voice_path_str}"
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


def normalize_forward_nodes(messages) -> List[Dict]:
    """把 ``get_forward_msg`` 返回的节点列表规范成可再次发送的 node 段。

    协议端返回的节点结构有两种常见形态：

    1. ``{"type": "node", "data": {...}}``
    2. ``{"sender": {"nickname", "user_id"}, "content"/"message": [...], "time": ...}``

    统一输出 ``{"type": "node", "data": {name, uin, content, time}}``，
    可直接用于 ``send_group_forward_msg`` / ``send_private_forward_msg``。
    """
    nodes: List[Dict] = []
    if not isinstance(messages, list):
        return nodes

    for item in messages:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "node":
            node_data = item.get("data") or {}
            uin = node_data.get("uin") or node_data.get("user_id") or ""
            name = (
                node_data.get("name")
                or node_data.get("nickname")
                or str(uin)
                or "用户"
            )
            content = node_data.get("content") or node_data.get("message") or []
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
                "uin": str(uin) if uin else str(FALLBACK_NODE_UIN),
                "content": content,
            },
        }
        if time_val is not None:
            try:
                node["data"]["time"] = int(time_val)
            except (TypeError, ValueError):
                pass
        nodes.append(node)

    return nodes


def extract_plain_text(components: list) -> str:
    """提取消息中的纯文本内容（含 @），用于关键词匹配。"""
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


def describe_message_type(
    components: list | None = None, *, is_forward: bool = False
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


def message_to_plain_text(message) -> str:
    """将 go-cqhttp 消息数组压缩成便于兜底通知的纯文本。"""
    if isinstance(message, str):
        return message.strip()
    if not isinstance(message, list):
        return str(message).strip()

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
            parts.append(
                GOCQ_PART_PLACEHOLDER.get(part_type, f"[{part_type or '未知内容'}]")
            )

    merged = "\n".join(part for part in parts if part).strip()
    return merged


__all__ = [
    "serialize_components",
    "deserialize_components",
    "component_to_gocq_parts",
    "normalize_forward_nodes",
    "extract_plain_text",
    "describe_message_type",
    "message_to_plain_text",
]
