"""收到群消息 → 写入缓存。

缓存以「消息 ID」为键落盘，撤回事件到来时由 recovery 读取。
"""

import asyncio
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.core.message.message_event_result import MessageChain

from .components import serialize_components
from .utils import build_cache_task_key, delayed_delete


class CacheFlowMixin:
    """消息缓存侧的入口与后台 worker。"""

    async def _handle_message_cache(self, event: AstrMessageEvent):
        """处理消息缓存，后台执行缓存任务"""
        group_id = str(event.get_group_id())
        message_id = str(event.message_obj.message_id)
        sender_id = str(event.get_sender_id())
        if self.settings.should_skip_cache(group_id, event.get_message_type()):
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
        if self.settings.contains_ignore_keyword(components):
            logger.debug(
                f"[{self.instance_id}] 消息包含忽略关键词，跳过缓存 "
                f"(群: {group_id}, 发送者: {sender_id})"
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
        """后台缓存任务：中转合并转发 → 缓存媒体 → 落盘。"""
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

        # 原消息本身是「聊天记录」时先留一份中转副本，撤回后据此重建
        relay_info = await self._relay_forward_message(
            client, group_id, message_id, sender_id, timestamp, raw_message
        )

        media = await self._cache_message_media(client, components, raw_message)

        file_path = (
            cache_file
            if isinstance(cache_file, Path)
            else self._get_cache_file_path(group_id, message_id)
        )
        serialized_components = serialize_components(components)
        for comp_dict in serialized_components:
            ctype = comp_dict.get("type")
            if ctype == "Image":
                url = comp_dict.get("url")
                if url in media.image_cache:
                    comp_dict["url"] = media.image_cache[url]
            elif ctype in ("Video", "Record"):
                file_val = comp_dict.get("file")
                if file_val in media.file_cache:
                    comp_dict["file"] = media.file_cache[file_val]
        self._write_cache_record(
            file_path,
            {
                "status": "done",
                "group_id": group_id,
                "message_id": message_id,
                "sender_id": sender_id,
                "timestamp": timestamp,
                "components": serialized_components,
                "local_file_map": media.local_file_map,
                "relay_info": relay_info,
            },
        )

        asyncio.create_task(
            delayed_delete(self.settings.cache_expiration_time, file_path)
        )
        return {"file_path": str(file_path), "relay_info": relay_info}
