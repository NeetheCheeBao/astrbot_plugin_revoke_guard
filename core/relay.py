"""合并转发消息（聊天记录）的中转与节点预缓存。

原消息本身是「聊天记录」时，一旦被撤回，原 ``message_id`` 就失效了，
所以必须在**收到消息时**先把内容留一份：

1. 用 ``get_forward_msg`` 取出节点内容（此时消息还在，最可靠），
   节点会随缓存记录一起落盘，撤回后可直接重建发送；
2. 再按配置把这份聊天记录重建一份副本，转发到 Bot 自身或中转群，
   供「中转留档」与自动撤回使用。

**不要用 ``forward_*_single_msg`` 去转发「聊天记录」**：协议端这类调用会走
``forwardMsgWithComment``，在合并转发上直接超时（``retcode 1200 invoke timeout``），
既拿不到 ``message_id`` 也留不下副本。因此节点重建优先，旧接口只作为
「取不到节点」时的兜底。
"""

import asyncio

from astrbot.api import logger

from .components import normalize_forward_nodes


def extract_message_id(ret) -> str:
    """从协议端返回值里取出 message_id（兼容 data 包裹）。"""
    if not isinstance(ret, dict):
        return ""
    data = ret.get("data")
    data = data if isinstance(data, dict) else {}
    return str(ret.get("message_id") or data.get("message_id") or "")


class RelayMixin:
    """把「聊天记录」类型的中转副本信息写入缓存记录。"""

    @staticmethod
    def _is_forward_message(raw_message) -> bool:
        """原始消息是否是一条合并转发（聊天记录）。"""
        if not isinstance(raw_message, dict):
            return False
        message_list = raw_message.get("message", [])
        if not isinstance(message_list, list) or not message_list:
            return False
        first_segment = message_list[0]
        return (
            isinstance(first_segment, dict)
            and first_segment.get("type") == "forward"
        )

    async def _fetch_forward_nodes(self, client, message_id: str) -> list | None:
        """用 ``get_forward_msg`` 取出聊天记录节点，失败返回 None。

        必须在原消息仍存在时调用（即收到消息后立即调用）。
        """
        message_id = str(message_id or "").strip()
        if not message_id:
            return None

        for attempt in range(2):
            try:
                detail = await client.api.call_action(
                    "get_forward_msg", message_id=message_id
                )
            except Exception as exc:
                logger.warning(
                    f"[{self.instance_id}] 获取聊天记录节点失败 "
                    f"(ID: {message_id}, attempt {attempt + 1}/2): {exc}"
                )
                if attempt == 0:
                    await asyncio.sleep(0.5)
                continue

            data = detail.get("data") if isinstance(detail, dict) else None
            if not isinstance(data, dict):
                data = detail if isinstance(detail, dict) else {}
            nodes = normalize_forward_nodes(data.get("messages") or [])
            if nodes:
                logger.debug(
                    f"[{self.instance_id}] 已缓存聊天记录节点 "
                    f"(ID: {message_id}, 节点数: {len(nodes)})"
                )
                return nodes

            if attempt == 0:
                await asyncio.sleep(0.5)

        logger.warning(
            f"[{self.instance_id}] 聊天记录节点为空，无法预缓存 (ID: {message_id})"
        )
        return None

    async def _relay_forward_message(
        self,
        client,
        group_id: str,
        message_id: str,
        sender_id: str,
        timestamp: int,
        is_forward: bool,
        forward_nodes: list | None = None,
    ) -> dict | None:
        """原消息是合并转发时做中转，返回 relay_info（无需中转时返回 None）。

        ``forward_nodes`` 由调用方预先取好；为 None 表示节点没取到，
        此时才回退到 ``forward_*_single_msg``。
        """
        if not is_forward:
            return None

        relay_info = None
        if self.settings.forward_to_self:
            relay_info = await self._relay_to_self(
                client, group_id, message_id, sender_id, timestamp, forward_nodes
            )
        if not relay_info and self.settings.forward_relay_group:
            relay_info = await self._relay_to_group(
                client, group_id, message_id, sender_id, timestamp, forward_nodes
            )
        return relay_info

    async def _forward_merged_copy(
        self,
        client,
        target_type: str,
        target_id_str: str,
        forward_nodes: list | None,
        message_id: str,
    ):
        """把一份聊天记录副本转发到目标会话。

        有节点时用 ``send_*_forward_msg`` 重建（稳定，且能拿到 message_id）；
        没节点时才退回 ``forward_*_single_msg``。
        """
        if target_type == "private":
            params = {"user_id": int(target_id_str)}
            rebuild_action = "send_private_forward_msg"
            single_action = "forward_friend_single_msg"
        else:
            params = {"group_id": int(target_id_str)}
            rebuild_action = "send_group_forward_msg"
            single_action = "forward_group_single_msg"

        if forward_nodes:
            try:
                return await client.api.call_action(
                    rebuild_action, messages=forward_nodes, **params
                )
            except Exception as exc:
                logger.warning(
                    f"[{self.instance_id}] 用节点重建聊天记录失败，"
                    f"回退单条转发: {exc}"
                )
        return await client.api.call_action(
            single_action, message_id=message_id, **params
        )

    async def _relay_to_self(
        self,
        client,
        group_id: str,
        message_id: str,
        sender_id: str,
        timestamp: int,
        forward_nodes: list | None,
    ) -> dict | None:
        """转发一份聊天记录副本到 Bot 自身私聊。"""
        for attempt in range(2):
            try:
                self_id = await self._get_self_id(client)
                if not self_id:
                    break
                ret = await self._forward_merged_copy(
                    client, "private", str(self_id), forward_nodes, message_id
                )
                relay_msg_id = extract_message_id(ret)
                if not relay_msg_id:
                    await asyncio.sleep(1)
                    relay_msg_id = await self._find_private_relay_msg_id(
                        client, self_id, timestamp
                    )
                if relay_msg_id:
                    return {
                        "relay_msg_id": relay_msg_id,
                        "sender_id": sender_id,
                        "timestamp": timestamp,
                        "group_id": group_id,
                        "is_private_relay": True,
                    }
            except Exception as exc:
                logger.warning(
                    f"[{self.instance_id}] 转发合并消息给机器人自身失败 "
                    f"(attempt {attempt + 1}/2): {exc}"
                    "（节点缓存已留存，撤回时仍可重建）"
                )
                if attempt == 0:
                    self._cached_self_id = None
                else:
                    break
        return None

    async def _find_private_relay_msg_id(
        self, client, self_id: str, timestamp: int
    ) -> str:
        """协议端没返回 message_id 时，从自身私聊历史里倒查中转副本。"""
        try:
            history_result = await client.api.call_action(
                "get_friend_msg_history", user_id=int(self_id), count=10
            )
        except Exception as exc:
            logger.warning(
                f"[{self.instance_id}] 查询自身私聊历史消息失败: {exc}"
            )
            return ""

        messages = []
        if isinstance(history_result, dict):
            messages = history_result.get("messages", []) or history_result.get(
                "data", {}
            ).get("messages", [])
        for msg in reversed(messages):
            msg_time = int(msg.get("time", 0))
            if abs(msg_time - timestamp) <= 2:
                return str(msg.get("message_id") or "")
        return ""

    async def _relay_to_group(
        self,
        client,
        group_id: str,
        message_id: str,
        sender_id: str,
        timestamp: int,
        forward_nodes: list | None,
    ) -> dict | None:
        """转发一份聊天记录副本到中转群。"""
        relay_group = self.settings.forward_relay_group
        try:
            ret = await self._forward_merged_copy(
                client, "group", str(relay_group), forward_nodes, message_id
            )
        except Exception as exc:
            logger.warning(
                f"[{self.instance_id}] 转发合并消息到中转群失败 "
                f"(中转群: {relay_group}): {exc}"
                "（节点缓存已留存，撤回时仍可重建）"
            )
            return None

        relay_msg_id = extract_message_id(ret)
        relay_msg_time = None
        if not relay_msg_id:
            relay_msg_id, relay_msg_time = await self._find_group_relay_msg_id(
                client, timestamp
            )
        if not relay_msg_id:
            return None

        if self.settings.auto_recall_relay:
            asyncio.create_task(
                self._auto_recall_relay_msg(client, relay_msg_id)
            )
        return {
            "relay_msg_id": relay_msg_id,
            "sender_id": sender_id,
            "timestamp": timestamp,
            "relay_timestamp": relay_msg_time,
            "group_id": group_id,
        }

    async def _find_group_relay_msg_id(
        self, client, timestamp: int
    ) -> tuple[str, int | None]:
        """协议端没返回 message_id 时，从中转群历史里倒查中转副本。"""
        self_id = await self._get_self_id(client)

        for attempt in range(2):
            await asyncio.sleep(1)
            try:
                relay_group_int = int(self.settings.forward_relay_group)
                found_msg = None
                next_seq = 0
                for _ in range(5):
                    history_result = await client.api.call_action(
                        "get_group_msg_history",
                        group_id=relay_group_int,
                        message_seq=next_seq,
                        count=20,
                    )
                    messages = []
                    if isinstance(history_result, dict):
                        data = history_result.get("data")
                        if isinstance(data, dict):
                            messages = data.get("messages") or []
                        if not messages:
                            messages = history_result.get("messages") or []
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
                    if (
                        timestamp - oldest_time
                        > self.settings.cache_expiration_time
                    ):
                        break
                    if next_seq == 0:
                        break

                if found_msg:
                    return (
                        str(found_msg.get("message_id") or ""),
                        found_msg.get("time"),
                    )
            except Exception as exc:
                logger.warning(
                    f"[{self.instance_id}] 查询中转群历史消息失败 "
                    f"(attempt {attempt + 1}/2): {exc}"
                )

            if attempt == 0:
                self._cached_self_id = None

        return "", None
