"""配置解析与基于配置的过滤规则。

AstrBot 把插件配置以 dict 传入 ``Star`` 子类，这里统一解析为 ``PluginSettings``，
避免各处再关心字段兼容与默认值。
"""

from dataclasses import dataclass, field
from typing import List

from astrbot.api.platform import MessageType

from .components import extract_plain_text
from .constants import (
    DEFAULT_CACHE_EXPIRATION_TIME,
    DEFAULT_FILE_SIZE_THRESHOLD_MB,
)

# 撤回事件与消息事件之间最多等待多久去读缓存记录（秒）。
DEFAULT_CACHE_PENDING_TIMEOUT = 2

# 下载文件时的重试策略。
DEFAULT_FILE_DOWNLOAD_RETRY_ATTEMPTS = 3
DEFAULT_FILE_DOWNLOAD_RETRY_DELAY = 0.2


@dataclass
class PluginSettings:
    """插件运行期使用的全部配置。"""

    monitor_mode: str = "所有群聊"
    whitelist_groups: List[str] = field(default_factory=list)
    blacklist_groups: List[str] = field(default_factory=list)
    ignore_operators: List[str] = field(default_factory=list)
    ignore_keywords: List[str] = field(default_factory=list)

    notify_mode: str = "发送到群聊"
    target_receivers: List[str] = field(default_factory=list)
    target_groups: List[str] = field(default_factory=list)

    cache_expiration_time: int = DEFAULT_CACHE_EXPIRATION_TIME
    cache_pending_timeout: float = DEFAULT_CACHE_PENDING_TIMEOUT
    file_size_threshold_mb: int = DEFAULT_FILE_SIZE_THRESHOLD_MB

    file_download_retry_attempts: int = DEFAULT_FILE_DOWNLOAD_RETRY_ATTEMPTS
    file_download_retry_delay: float = DEFAULT_FILE_DOWNLOAD_RETRY_DELAY

    relay_mode: str = "中转给Bot自身"
    forward_to_self: bool = True
    forward_relay_group: str = ""
    auto_recall_relay: bool = True

    # 恒为 True：默认始终伪造合并转发恢复，不再提供开关。
    enable_fake_forward: bool = True

    @classmethod
    def from_config(cls, config: dict | None) -> "PluginSettings":
        """从 AstrBot 传入的配置字典解析出设置对象。"""
        config = config or {}
        monitor = config.get("monitor", {}) or {}
        target = config.get("target", {}) or {}
        cache = config.get("cache", {}) or {}
        forward = config.get("forward", {}) or {}

        monitor_mode = str(
            monitor.get("monitor_mode")
            or ("白名单群聊" if monitor.get("monitor_groups") else "所有群聊")
        )
        whitelist_groups = [
            str(g)
            for g in (
                monitor.get("whitelist_groups")
                or monitor.get("monitor_groups")
                or []
            )
            if str(g).strip()
        ]
        blacklist_groups = [
            str(g) for g in (monitor.get("blacklist_groups") or []) if str(g).strip()
        ]
        # 黑名单用户：其撤回行为不触发通知（兼容旧字段 ignore_operators）
        ignore_operators = [
            str(o)
            for o in (
                monitor.get("blacklist_users")
                or monitor.get("ignore_operators")
                or []
            )
        ]
        ignore_keywords = [
            str(k).strip()
            for k in (monitor.get("ignore_keywords", []) or [])
            if str(k).strip()
        ]

        notify_mode = str(target.get("notify_mode") or "发送到群聊")
        target_receivers = [str(r) for r in target.get("target_receivers", []) or []]
        target_groups = [str(g) for g in target.get("target_groups", []) or []]

        cache_expiration_time = int(
            cache.get("cache_expiration_time", DEFAULT_CACHE_EXPIRATION_TIME)
        )
        file_size_threshold_mb = int(
            cache.get("file_size_threshold_mb", DEFAULT_FILE_SIZE_THRESHOLD_MB)
        )

        relay_mode, forward_to_self, forward_relay_group = cls._resolve_relay_mode(
            forward
        )

        return cls(
            monitor_mode=monitor_mode,
            whitelist_groups=whitelist_groups,
            blacklist_groups=blacklist_groups,
            ignore_operators=ignore_operators,
            ignore_keywords=ignore_keywords,
            notify_mode=notify_mode,
            target_receivers=target_receivers,
            target_groups=target_groups,
            cache_expiration_time=cache_expiration_time,
            cache_pending_timeout=min(
                cache_expiration_time, DEFAULT_CACHE_PENDING_TIMEOUT
            ),
            file_size_threshold_mb=file_size_threshold_mb,
            relay_mode=relay_mode,
            forward_to_self=forward_to_self,
            forward_relay_group=forward_relay_group,
            auto_recall_relay=forward.get("auto_recall_relay", True),
        )

    @staticmethod
    def _resolve_relay_mode(forward: dict) -> tuple[str, bool, str]:
        """解析「中转方式」，兼容各种存储值 / 旧版 bool。"""
        relay_mode_raw = str(forward.get("relay_mode") or "").strip()
        relay_group_cfg = str(forward.get("forward_relay_group", "") or "").strip()
        if (
            "群聊" in relay_mode_raw
            or relay_mode_raw in ("group", "relay_group", "to_group")
        ):
            return "中转到群聊", False, relay_group_cfg
        if (
            "自身" in relay_mode_raw
            or "Bot" in relay_mode_raw
            or relay_mode_raw in ("self", "bot", "to_self")
        ):
            return "中转给Bot自身", True, ""
        if relay_group_cfg:
            # 未选中转方式但填了中转群号 → 按中转到群聊
            return "中转到群聊", False, relay_group_cfg
        if forward.get("forward_to_self", False):
            return "中转给Bot自身", True, ""
        return "中转给Bot自身", True, ""

    def is_monitored(self, group_id: str) -> bool:
        """按监控模式判断该群是否受监控。"""
        mode = self.monitor_mode
        gid = str(group_id)
        if mode == "白名单群聊":
            return gid in self.whitelist_groups
        if mode == "黑名单群聊":
            return gid not in self.blacklist_groups
        # 所有群聊
        return True

    def should_skip_cache(self, group_id: str, message_type) -> bool:
        """非群聊消息或未被监控的群不缓存。"""
        return message_type != MessageType.GROUP_MESSAGE or not self.is_monitored(
            group_id
        )

    def targets(self) -> List[tuple]:
        """根据通知位置返回通知目标（私聊 / 群聊）。"""
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

    def contains_ignore_keyword(self, components: list) -> bool:
        """消息文本是否包含任一忽略关键词。"""
        if not self.ignore_keywords:
            return False
        text = extract_plain_text(components)
        if not text:
            return False
        for keyword in self.ignore_keywords:
            if keyword and keyword in text:
                return True
        return False
