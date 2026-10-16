"""与具体业务无关的通用小工具。"""

import asyncio
import os
import time
import traceback
from pathlib import Path
from typing import List

from astrbot.api import logger


def get_value(obj, key, default=None):
    """从 dict 或对象上取值，取值失败时返回默认值。"""
    try:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)
    except Exception:
        return default


def build_cache_task_key(group_id: str, message_id: str) -> str:
    """缓存任务在内存中的唯一键。"""
    return f"{group_id}:{message_id}"


def get_private_unified_msg_origin(user_id: str, platform: str = "aiocqhttp") -> str:
    """构造私聊会话的 unified_msg_origin。"""
    return f"{platform}:FriendMessage:{user_id}"


def now_ms() -> int:
    """当前毫秒时间戳。"""
    return int(time.time() * 1000)


async def delayed_delete(delay: int, path: Path):
    """延时删除文件，失败只记录日志。"""
    await asyncio.sleep(delay)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        logger.error(f"[RevokeGuard] 删除文件失败 ({path}): {traceback.format_exc()}")


async def cleanup_local_files(file_paths: List[str]):
    """清理本次发送过程中临时落盘的文件。"""
    if not file_paths:
        return
    await asyncio.sleep(1)
    for abs_path in file_paths:
        try:
            os.remove(abs_path)
        except Exception as e:
            logger.error(f"[RevokeGuard] ❌ 清理本地文件失败 ({abs_path}): {e}")
