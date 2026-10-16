"""合并转发消息的中转。

原消息本身是「聊天记录」时，先把它转发到 Bot 自身或中转群留一份副本；
撤回后即可用这份副本重建聊天记录，避免出现「该消息类型暂不支持查看」。
"""

import asyncio
import traceback

from astrbot.api import logger


class RelayMixin:
    """把「聊天记录」类型的中转副本信息写入缓存记录。"""

    async def _relay_forward_message(
        self,
        client,
        group_id: str,
        message_id: str,
        sender_id: str,
        timestamp: int,
        raw_message: dict,
    ) -> dict | None:
        """原消息是合并转发时做中转，返回 relay_info（无需中转时返回 None）。"""
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

        if is_forward and self.settings.forward_to_self:
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
                        f"[{self.instance_id}] ❌ 转发合并消息给机器人自身失败 "
                        f"(attempt {attempt + 1}/2): {exc}"
                    )
                    if attempt == 0:
                        self._cached_self_id = None
                    else:
                        break

        if not relay_info and is_forward and self.settings.forward_relay_group:
            try:
                ret = await client.api.call_action(
                    "forward_group_single_msg",
                    group_id=int(self.settings.forward_relay_group),
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
                                    group_id=int(self.settings.forward_relay_group),
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
                                if (
                                    timestamp - oldest_time
                                    > self.settings.cache_expiration_time
                                ):
                                    break
                                if next_seq == 0:
                                    break

                            if found_msg:
                                relay_msg_id = found_msg.get("message_id")
                                relay_msg_time = found_msg.get("time")
                                break
                        except Exception as exc:
                            logger.warning(
                                f"[{self.instance_id}] 查询历史消息失败 "
                                f"(attempt {attempt + 1}/2): {exc}"
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
                    if self.settings.auto_recall_relay:
                        asyncio.create_task(
                            self._auto_recall_relay_msg(client, relay_msg_id)
                        )
            except Exception as exc:
                logger.error(
                    f"[{self.instance_id}] ❌ 转发合并消息到中转群失败: "
                    f"{exc}\n{traceback.format_exc()}"
                )

        return relay_info
