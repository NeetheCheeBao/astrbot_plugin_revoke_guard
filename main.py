"""astrbot_plugin_revoke_guard 插件入口。

AstrBot 以 ``cls.__module__`` 注册插件类、以 ``handler.__module__`` 绑定事件处理器，
所以插件类与带 ``@filter`` 装饰器的处理器必须留在本文件；
其余实现按职责拆分到 ``core/`` 包，由下面的混入（mixin）组合进来。
"""

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType

from .core.cache_flow import CacheFlowMixin
from .core.media import MediaMixin
from .core.onebot import OneBotMixin
from .core.recovery import RecoveryMixin
from .core.relay import RelayMixin
from .core.sender import SenderMixin
from .core.settings import PluginSettings
from .core.storage import CacheTaskState, StorageMixin


class RevokeGuard(
    CacheFlowMixin,
    MediaMixin,
    OneBotMixin,
    RecoveryMixin,
    RelayMixin,
    SenderMixin,
    StorageMixin,
    Star,
):
    """QQ群消息撤回监控（防撤回）。

    本类只负责组装与注册：持有运行期状态、暴露两个事件处理器，
    具体逻辑分散在 ``core/`` 下的各个 mixin 中。
    """

    instance_id = "RevokeGuard"
    """日志前缀标识。"""

    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.settings = PluginSettings.from_config(config)
        self._setup_cache_paths()
        self._cached_self_id: str | None = None
        self._cache_tasks: dict[str, CacheTaskState] = {}
        self._cleanup_cache_on_startup()

    async def initialize(self) -> None:
        logger.info(
            f"[{self.instance_id}] 插件已加载。"
            f" 中转方式={self.settings.relay_mode},"
            f" 中转群={self.settings.forward_relay_group or '-'},"
            f" to_self={self.settings.forward_to_self}"
        )

    async def terminate(self) -> None:
        logger.info(f"[{self.instance_id}] 插件已卸载/重载。")

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=20)
    async def handle_message_cache(self, event: AstrMessageEvent):
        """缓存群消息，供撤回后恢复。"""
        return await self._handle_message_cache(event)

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=10)
    async def handle_recall_event(self, event: AstrMessageEvent):
        """处理群聊撤回事件。"""
        return await self._handle_recall_event(event)
