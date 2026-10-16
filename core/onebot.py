"""OneBot v11 协议端接口辅助。

这里的三个方法是对 ``client.api.call_action`` 的通用小封装，
不依赖任何具体协议端实现。
"""

import asyncio

from astrbot.api import logger


class OneBotMixin:
    """机器人自身账号、群名片与消息撤回的小封装。"""

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
        await asyncio.sleep(self.settings.cache_expiration_time)
        try:
            await client.api.call_action("delete_msg", message_id=relay_msg_id)
        except Exception as e:
            logger.error(
                f"[{self.instance_id}] 自动撤回中转群消息失败 (ID: {relay_msg_id}): {e}"
            )

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
