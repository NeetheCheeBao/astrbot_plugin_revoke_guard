"""收到撤回事件 → 恢复内容并通知。

合并转发（聊天记录）被撤回时，按下面的顺序重建一份发到通知目标：

1. 中转群 / Bot 自身私聊里的**中转副本**——平台在发送时重新锚定过媒体，
   记录里的图片视频最不容易失效，所以优先；
2. 收到消息时落盘的**节点缓存**——副本被自动撤回、或中转本身失败时的兜底；
3. 原消息 ID——撤回后基本已失效，只在极端情况下还能读到。

普通消息则走「伪造单节点聊天记录」，失败后再降级到单条转发或组件直发。
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent

from .components import (
    component_to_gocq_parts,
    describe_message_type,
    deserialize_components,
    message_to_plain_text,
)
from .constants import (
    SPECIAL_COMPONENT_TYPE_SET,
    SPECIAL_TYPE_CN,
    SUPPORTED_COMPONENT_TYPES,
)
from .utils import (
    build_cache_task_key,
    cleanup_local_files,
    delayed_delete,
    get_value,
)


@dataclass
class RecallContext:
    """一次撤回恢复的公共上下文。"""

    client: Any
    group_id: str
    message_id: str
    operator_id: str
    cached_data: dict[str, Any]
    file_path: Path | None


@dataclass
class RecallSender:
    """撤回提醒中要展示的群/发送者信息。"""

    sender_id: str
    nickname: str
    group_name: str
    operator_nickname: str
    timestamp: int | None


@dataclass
class PlainRecallJob:
    """普通消息（非纯聊天记录）的恢复任务。"""

    ctx: RecallContext
    sender: RecallSender
    components: list
    special_components: list
    other_components: list
    unsupported_types: set
    local_file_map: dict
    local_files_to_cleanup: list


class RecoveryMixin:
    """撤回事件侧的入口与三条恢复路径。"""

    async def _handle_recall_event(self, event: AstrMessageEvent):
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
            if not self.settings.is_monitored(group_id) or not message_id:
                return None
            if operator_id in self.settings.ignore_operators:
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
                    cache_key, timeout=self.settings.cache_expiration_time
                )
                if task_result and task_result.get("file_path"):
                    file_path = Path(task_result["file_path"])
                    cached_data = self._load_cached_data(file_path)

            if not cached_data:
                file_path = self._find_cache_file(group_id, message_id)
                cached_data = self._load_cached_data(file_path)

            if cached_data and cached_data.get("status") == "failed":
                logger.warning(
                    f"[{self.instance_id}] 消息缓存失败，跳过撤回恢复"
                    f"（ID: {message_id}）。"
                )
                return None
            if cached_data and cached_data.get("status") == "pending":
                logger.warning(
                    f"[{self.instance_id}] 消息缓存仍在进行中，跳过撤回恢复"
                    f"（ID: {message_id}）。"
                )
                return None

            # 如果没有找到缓存数据，则无法恢复
            if not cached_data:
                logger.warning(
                    f"[{self.instance_id}] 找不到消息记录 (ID: {message_id})，"
                    f"可能已过期或未缓存。"
                )
                return None

            ctx = RecallContext(
                client=client,
                group_id=group_id,
                message_id=message_id,
                operator_id=operator_id,
                cached_data=cached_data,
                file_path=file_path,
            )

            # 原消息本身就是「聊天记录/合并转发」时走此分支：
            # 原样转发聊天记录，不再嵌套伪造一层
            relay_info = cached_data.get("relay_info")
            forward_nodes = cached_data.get("forward_nodes")
            if relay_info or forward_nodes:
                await self._recover_forward_recall(
                    ctx, relay_info=relay_info, forward_nodes=forward_nodes
                )
                return None

            await self._recover_plain_recall(ctx)
        return None

    async def _recover_forward_recall(
        self,
        ctx: RecallContext,
        relay_info: dict | None = None,
        forward_nodes: list | None = None,
    ) -> None:
        """原消息是合并转发：重建聊天记录并补撤回提醒。

        数据源按「中转副本 → 节点缓存 → 原消息 ID」的顺序尝试，
        见 ``_rebuild_forward_record``。
        """
        logger.info(
            f"[{self.instance_id}] 检测到合并转发消息被撤回，"
            f"原消息ID: {ctx.message_id}"
        )
        relay_info = relay_info or {}
        sender_id = str(
            relay_info.get("sender_id") or ctx.cached_data.get("sender_id") or ""
        )
        timestamp = relay_info.get("timestamp") or ctx.cached_data.get("timestamp")

        relay_msg_id = str(relay_info.get("relay_msg_id") or "")

        try:
            client = ctx.client
            (
                group_name,
                member_nickname,
                operator_nickname,
            ) = await self._get_group_context_names(
                client,
                ctx.group_id,
                sender_id=sender_id,
                operator_id=ctx.operator_id,
            )

            logger.info(
                f"[{self.instance_id}] 合并转发撤回 - 群: {group_name}, "
                f"发送者: {member_nickname}, "
                f"操作者: {operator_nickname} ({ctx.operator_id}), "
                f"缓存节点: {len(forward_nodes) if forward_nodes else 0}"
            )

            targets = self.settings.targets()

            for target_type, target_id in targets:
                target_id_str = str(target_id)
                header = self._create_recall_notification_header(
                    group_name,
                    ctx.group_id,
                    member_nickname,
                    sender_id,
                    operator_nickname,
                    ctx.operator_id,
                    timestamp,
                    message_type=describe_message_type(is_forward=True),
                )

                forwarded_msg_id = await self._rebuild_forward_record(
                    client,
                    target_type,
                    target_id_str,
                    relay_msg_id,
                    forward_nodes,
                    ctx.message_id,
                )
                if forwarded_msg_id:
                    logger.info(
                        f"[{self.instance_id}] 已原样转发聊天记录到 "
                        f"{target_type} {target_id_str}"
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
                            f"[{self.instance_id}] 引用聊天记录发送撤回信息失败，"
                            f"降级为普通文本: {e}"
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
                        logger.error(f"[{self.instance_id}] 发送撤回信息失败: {e}")

            # 立即撤回中转群的消息 (如果是私聊中转则不撤回)
            if (
                self.settings.auto_recall_relay
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

    async def _rebuild_forward_record(
        self,
        client,
        target_type: str,
        target_id_str: str,
        relay_msg_id: str,
        forward_nodes: list | None,
        origin_msg_id: str,
    ) -> str | None:
        """按「中转副本 → 节点缓存 → 原消息」的顺序重建一条聊天记录。

        中转副本优先：它是在中转群/自身私聊里重新发出去的一条消息，
        平台发送时已经把媒体重新上传锚定过，记录里的图片视频最不容易失效。
        """
        if relay_msg_id:
            msg_id = await self._rebuild_forward_to_target(
                client, target_type, target_id_str, relay_msg_id
            )
            if msg_id:
                logger.info(
                    f"[{self.instance_id}] 已从中转副本重建聊天记录 "
                    f"(副本ID: {relay_msg_id})"
                )
                return msg_id

        if forward_nodes:
            msg_id = await self._send_forward_nodes_to_target(
                client, target_type, target_id_str, forward_nodes
            )
            if msg_id:
                logger.info(
                    f"[{self.instance_id}] 中转副本不可用，改用节点缓存重建聊天记录 "
                    f"({len(forward_nodes)} 个节点)"
                )
                return msg_id

        if origin_msg_id:
            msg_id = await self._rebuild_forward_to_target(
                client, target_type, target_id_str, origin_msg_id
            )
            if msg_id:
                logger.info(
                    f"[{self.instance_id}] 已按原消息 ID 重建聊天记录 "
                    f"({origin_msg_id})"
                )
                return msg_id

        # 最后一档：原生单条转发（部分协议端对合并转发也支持）
        return await self._forward_single_msg_to_target(
            client, target_type, target_id_str, [origin_msg_id]
        )

    async def _recover_plain_recall(self, ctx: RecallContext) -> None:
        """普通消息的撤回恢复。"""
        cached_data = ctx.cached_data
        local_files_to_cleanup: list[str] = []
        try:
            sender_id = cached_data["sender_id"]
            local_file_map = cached_data.get("local_file_map", {})
            if ctx.operator_id in self.settings.ignore_operators:
                logger.debug(
                    f"[{self.instance_id}] 操作人 {ctx.operator_id} 在忽略列表中，跳过处理"
                )
                return

            cached_components_data = cached_data.get("components", [])

            unsupported_types = set()
            for comp_dict in cached_components_data:
                comp_type_name = comp_dict.get("type")
                if comp_type_name not in SUPPORTED_COMPONENT_TYPES:
                    unsupported_types.add(comp_type_name)

            components = deserialize_components(cached_components_data)

            timestamp = cached_data.get("timestamp")
            (
                group_name,
                member_nickname,
                operator_nickname,
            ) = await self._get_group_context_names(
                ctx.client,
                ctx.group_id,
                sender_id=sender_id,
                operator_id=ctx.operator_id,
            )

            special_components = [
                comp
                for comp in components
                if getattr(comp.type, "name", "unknown")
                in SPECIAL_COMPONENT_TYPE_SET
            ]
            other_components = [
                comp
                for comp in components
                if getattr(comp.type, "name", "unknown")
                not in SPECIAL_COMPONENT_TYPE_SET
            ]

            job = PlainRecallJob(
                ctx=ctx,
                sender=RecallSender(
                    sender_id=sender_id,
                    nickname=member_nickname,
                    group_name=group_name,
                    operator_nickname=operator_nickname,
                    timestamp=timestamp,
                ),
                components=components,
                special_components=special_components,
                other_components=other_components,
                unsupported_types=unsupported_types,
                local_file_map=local_file_map,
                local_files_to_cleanup=local_files_to_cleanup,
            )

            # 纯聊天记录消息（无中转缓存时）：直接原样转发，避免伪造嵌套一层
            pure_forward = (
                len(special_components) == 1
                and not other_components
                and getattr(special_components[0].type, "name", "") == "Forward"
            )
            if pure_forward:
                await self._recover_pure_forward(job)
                return

            async with aiohttp.ClientSession() as session:
                targets = self.settings.targets()
                for target_type, target_id in targets:
                    await self._deliver_plain_recall(
                        session, job, target_type, str(target_id)
                    )
        finally:
            if local_files_to_cleanup:
                asyncio.create_task(cleanup_local_files(local_files_to_cleanup))
            if ctx.file_path:
                asyncio.create_task(delayed_delete(0, ctx.file_path))

    async def _recover_pure_forward(self, job: PlainRecallJob) -> None:
        """纯聊天记录消息：直接原样转发，不再嵌套伪造一层。"""
        ctx = job.ctx
        sender = job.sender
        targets = self.settings.targets()
        notification_prefix = self._create_recall_notification_header(
            sender.group_name,
            ctx.group_id,
            sender.nickname,
            sender.sender_id,
            sender.operator_nickname,
            ctx.operator_id,
            sender.timestamp,
            message_type=describe_message_type(is_forward=True),
        )
        for target_type, target_id in targets:
            target_id_str = str(target_id)
            forwarded_msg_id = await self._forward_single_msg_to_target(
                ctx.client,
                target_type,
                target_id_str,
                [ctx.message_id],
            )
            if not forwarded_msg_id:
                fallback_text = (
                    f"{notification_prefix}\n--------------------\n"
                    f"原消息为合并转发（聊天记录），原样转发失败。"
                )
                try:
                    await self._send_to_target(
                        ctx.client, target_type, target_id_str, fallback_text
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
                        ctx.client, target_type, target_id_str, reply_message
                    )
                except Exception:
                    try:
                        await self._send_to_target(
                            ctx.client, target_type, target_id_str, header
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.instance_id}] 发送撤回信息失败: {e}"
                        )
            else:
                try:
                    await self._send_to_target(
                        ctx.client, target_type, target_id_str, header
                    )
                except Exception as e:
                    logger.error(f"[{self.instance_id}] 发送撤回信息失败: {e}")

    async def _deliver_plain_recall(
        self,
        session: aiohttp.ClientSession,
        job: PlainRecallJob,
        target_type: str,
        target_id_str: str,
    ) -> None:
        """把一条普通消息的恢复内容发到单个目标会话。"""
        ctx = job.ctx
        sender = job.sender
        client = ctx.client
        components = job.components
        special_components = job.special_components
        other_components = job.other_components
        unsupported_types = job.unsupported_types
        local_file_map = job.local_file_map
        local_files_to_cleanup = job.local_files_to_cleanup

        notification_prefix = self._create_recall_notification_header(
            sender.group_name,
            ctx.group_id,
            sender.nickname,
            sender.sender_id,
            sender.operator_nickname,
            ctx.operator_id,
            sender.timestamp,
            message_type=describe_message_type(components),
        )
        warning_text = (
            f"\n⚠️ 注意：包含不支持的组件：{', '.join(unsupported_types)}"
            if unsupported_types
            else ""
        )

        fake_succeeded = False
        if self.settings.enable_fake_forward:
            # 开启伪造合并转发时：优先只发聊天记录，再引用补充撤回信息
            fake_msg_id = await self._send_fake_forward(
                client,
                target_type,
                target_id_str,
                sender.sender_id,
                sender.nickname,
                sender.timestamp,
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
                        f"[{self.instance_id}] 引用聊天记录发送撤回信息失败，"
                        f"降级为普通文本: {e}"
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
                getattr(c.type, "name", "") for c in components
            }
            prefer_direct = bool(
                special_type_names & {"Json", "Record", "Video", "File", "Forward"}
            )
            if not prefer_direct:
                try:
                    sf_id = await self._forward_single_msg_to_target(
                        client,
                        target_type,
                        target_id_str,
                        [ctx.message_id],
                    )
                    if sf_id:
                        single_forwarded = True
                        header_text = f"{notification_prefix}{warning_text}"
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
                return

            # 组件直发（伪造失败后的最终回退）
            if not special_components:
                message_parts = []
                for comp in other_components:
                    converted_parts = await component_to_gocq_parts(
                        comp,
                        session,
                        self.temp_path,
                        local_files_to_cleanup,
                        local_file_map,
                        show_qq_in_at=True,
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
                                    "text": f"{sender.nickname}："
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

                final_prefix_text = (
                    f"{notification_prefix}{warning_text}\n--------------------\n"
                )
                gocq_content_array = [
                    {
                        "type": "text",
                        "data": {"text": final_prefix_text},
                    }
                ]
                gocq_content_array.extend(final_message_parts)

                if len(gocq_content_array) > 1 or warning_text:
                    fallback_text = message_to_plain_text(gocq_content_array)
                    if not fallback_text:
                        fallback_text = (
                            f"{notification_prefix}{warning_text}\n--------------------\n"
                            f"[原消息发送失败，且无法提取可展示内容]"
                        )
                    await self._send_with_text_fallback(
                        client,
                        target_type,
                        target_id_str,
                        gocq_content_array,
                        "合并消息转发",
                        fallback_text,
                    )
            else:
                final_notification_text = (
                    f"{notification_prefix}{warning_text}\n"
                    f"--------------------\n内容将分条发送。"
                )
                header_ok = await self._send_with_text_fallback(
                    client,
                    target_type,
                    target_id_str,
                    final_notification_text,
                    "发送通知头",
                    f"{notification_prefix}\n[原始通知头发送失败，已切换为简化文本通知]",
                )
                if not header_ok:
                    return

                await asyncio.sleep(0.5)

                if other_components:
                    message_parts = []
                    for comp in other_components:
                        converted_parts = await component_to_gocq_parts(
                            comp,
                            session,
                            self.temp_path,
                            local_files_to_cleanup,
                            local_file_map,
                            show_qq_in_at=True,
                        )
                        message_parts.extend(converted_parts)
                    if message_parts:
                        content_message = [
                            {
                                "type": "text",
                                "data": {"text": f"{sender.nickname}："},
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
                        fallback_text = message_to_plain_text(content_message)
                        if not fallback_text:
                            fallback_text = (
                                f"{sender.nickname}："
                                f"[非特殊内容发送失败，且无法提取文本]"
                            )
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
                    content_parts = await component_to_gocq_parts(
                        comp,
                        session,
                        self.temp_path,
                        local_files_to_cleanup,
                        local_file_map,
                        show_qq_in_at=True,
                    )
                    final_parts_to_send = content_parts
                    if not other_components:
                        prefix_part = [
                            {
                                "type": "text",
                                "data": {"text": f"{sender.nickname}："},
                            }
                        ]
                        final_parts_to_send = prefix_part + content_parts

                    cn_type = SPECIAL_TYPE_CN.get(comp_type_name, comp_type_name)
                    fallback_text = message_to_plain_text(final_parts_to_send)
                    if (
                        not fallback_text
                        or fallback_text == f"{sender.nickname}："
                    ):
                        fallback_text = (
                            f"{sender.nickname}：[发送失败: {cn_type} "
                            f"消息可能包含无法上传的内容、过大或平台暂时超时]"
                        )
                    await self._send_with_text_fallback(
                        client,
                        target_type,
                        target_id_str,
                        final_parts_to_send,
                        f"发送特殊内容 ({comp_type_name})",
                        fallback_text,
                    )
