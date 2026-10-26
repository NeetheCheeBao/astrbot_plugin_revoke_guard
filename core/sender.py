"""发送层：撤回提醒、伪造聊天记录、单条转发与文本兜底。"""

import time
import traceback

import aiohttp

from astrbot.api import logger

from .components import component_to_gocq_parts, normalize_forward_nodes
from .constants import FALLBACK_NODE_UIN


class SenderMixin:
    """所有与「把内容发到目标会话」相关的实现。"""

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
            f"[{self.instance_id}] ❌ {context}失败到 {target_type} {target_id_str}："
            f"{original_error}\n{original_trace}"
        )
        try:
            await self._send_to_target(
                client, target_type, target_id_str, fallback_text
            )
            logger.warning(
                f"[{self.instance_id}] ⚠️ {context}失败，已向 {target_type} "
                f"{target_id_str} 发送文本兜底通知。"
            )
        except Exception as fallback_error:
            fallback_trace = "".join(
                traceback.format_exception(
                    type(fallback_error), fallback_error, fallback_error.__traceback__
                )
            )
            logger.error(
                f"[{self.instance_id}] ❌ {context}的文本兜底通知也失败到 {target_type} "
                f"{target_id_str}：{fallback_error}\n{fallback_trace}"
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
                    parts = await component_to_gocq_parts(
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
            # 部分协议端以 user_id(数值)+nickname 识别节点发言人，
            # 另有实现用 uin/name，故两组字段都填
            sender_uin = str(sender_id or "").strip() or str(FALLBACK_NODE_UIN)
            sender_name = str(member_nickname or sender_uin).strip() or sender_uin
            try:
                sender_uin_int = int(sender_uin)
            except (TypeError, ValueError):
                sender_uin_int = FALLBACK_NODE_UIN
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
                f"[{self.instance_id}] 伪造合并转发发送失败 "
                f"({target_type}/{target_id_str}): {e}"
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

    async def _send_forward_nodes_to_target(
        self,
        client,
        target_type: str,
        target_id_str: str,
        nodes: list,
    ) -> str | None:
        """把已缓存的聊天记录节点直接重建发送到目标会话。

        不依赖原消息是否还存在，也不需要中转副本，是撤回恢复的首选路径。
        """
        if not nodes:
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
        except Exception as e:
            logger.warning(
                f"[{self.instance_id}] 按缓存节点重建聊天记录失败 "
                f"-> {target_type} {target_id_str}: {e}"
            )
            return None
        msg_id = None
        if isinstance(ret, dict):
            _ret_data = ret.get("data", {}) or {}
            msg_id = (
                ret.get("message_id")
                or _ret_data.get("message_id")
                or ""
            )
        logger.info(
            f"[{self.instance_id}] 已按缓存节点重建聊天记录到 {target_type} "
            f"{target_id_str} (nodes={len(nodes)})"
        )
        return str(msg_id) if msg_id else "ok"

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

        data = detail.get("data", detail) if isinstance(detail, dict) else {}
        nodes = normalize_forward_nodes((data or {}).get("messages") or [])
        if not nodes:
            logger.warning(
                f"[{self.instance_id}] 无法从 get_forward_msg 构造节点 "
                f"(src={source_msg_id})"
            )
            return None

        return await self._send_forward_nodes_to_target(
            client, target_type, target_id_str, nodes
        )
