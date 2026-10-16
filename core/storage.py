"""缓存目录、缓存记录文件与缓存任务的调度与轮询。"""

import asyncio
import json
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.star import StarTools

from .utils import delayed_delete


@dataclass
class CacheTaskState:
    """一个后台缓存任务的状态。"""

    task: asyncio.Task | None
    done: asyncio.Event = field(default_factory=asyncio.Event)
    result: dict[str, Any] | None = None
    error: str | None = None
    started_at: float = field(default_factory=time.time)


class StorageMixin:
    """负责缓存落盘、任务调度与撤回事件侧的轮询读取。"""

    def _setup_cache_paths(self) -> None:
        """初始化数据目录与各媒体子目录。"""
        self.temp_path = Path(StarTools.get_data_dir("astrbot_plugin_revoke_guard"))
        self.temp_path.mkdir(exist_ok=True)
        self.video_cache_path = self.temp_path / "videos"
        self.video_cache_path.mkdir(exist_ok=True)
        self.voice_cache_path = self.temp_path / "voices"
        self.voice_cache_path.mkdir(exist_ok=True)
        self.file_cache_path = self.temp_path / "files"
        self.file_cache_path.mkdir(exist_ok=True)

    def _cleanup_cache_on_startup(self):
        """启动时清掉超过保留时长的残留缓存文件。"""
        now = time.time()
        expired_count = 0
        for cache_dir in [
            self.video_cache_path,
            self.voice_cache_path,
            self.file_cache_path,
            self.temp_path,
        ]:
            for file in cache_dir.glob("*"):
                if file.is_dir():
                    continue
                try:
                    if (
                        now - file.stat().st_mtime
                        > self.settings.cache_expiration_time
                    ):
                        file.unlink(missing_ok=True)
                        expired_count += 1
                except Exception:
                    continue
        logger.info(
            f"[{self.instance_id}] 缓存清理完成，移除了 {expired_count} 个过期文件。"
        )

    def _get_cache_file_path(self, group_id: str, message_id: str) -> Path:
        return self.temp_path / f"cache_{group_id}_{message_id}.json"

    def _find_cache_file(self, group_id: str, message_id: str) -> Path | None:
        file_path = self._get_cache_file_path(group_id, message_id)
        if file_path.exists():
            return file_path
        return next(self.temp_path.glob(f"*_{group_id}_{message_id}.json"), None)

    def _write_cache_record(self, file_path: Path, data: dict[str, Any]) -> None:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_cached_data(self, file_path: Path | None) -> dict[str, Any] | None:
        if not file_path or not file_path.exists():
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"[{self.instance_id}] 读取或解析本地缓存失败: {e}")
            return None

    async def _wait_for_cache_task_result(
        self, cache_key: str, timeout: float | None = None
    ) -> dict[str, Any] | None:
        state = self._cache_tasks.get(cache_key)
        if not state:
            return None
        if not state.done.is_set():
            try:
                await asyncio.wait_for(state.done.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f"[{self.instance_id}] 缓存任务仍在进行中: {cache_key}")
                return None
        return state.result

    async def _expire_cache_task_state(self, cache_key: str, state: CacheTaskState):
        await asyncio.sleep(self.settings.cache_expiration_time)
        current_state = self._cache_tasks.get(cache_key)
        if current_state is state:
            self._cache_tasks.pop(cache_key, None)

    async def _run_cache_task(
        self, cache_key: str, state: CacheTaskState, payload: dict[str, Any]
    ):
        file_path = payload.get("file_path")
        try:
            state.result = await self._cache_message_worker(payload)
        except Exception as e:
            state.error = str(e)
            if file_path:
                self._write_cache_record(
                    file_path,
                    {
                        "status": "failed",
                        "group_id": payload.get("group_id"),
                        "message_id": payload.get("message_id"),
                        "sender_id": payload.get("sender_id"),
                        "timestamp": payload.get("timestamp"),
                        "error": str(e),
                    },
                )
                asyncio.create_task(
                    delayed_delete(self.settings.cache_expiration_time, file_path)
                )
            logger.error(
                f"[{self.instance_id}] 缓存任务失败 ({cache_key}): "
                f"{e}\n{traceback.format_exc()}"
            )
        finally:
            state.task = None
            state.done.set()
            asyncio.create_task(self._expire_cache_task_state(cache_key, state))

    def _schedule_cache_task(
        self, cache_key: str, payload: dict[str, Any]
    ) -> CacheTaskState:
        existing_state = self._cache_tasks.get(cache_key)
        if existing_state and not existing_state.done.is_set():
            return existing_state

        state = CacheTaskState(task=None)
        self._cache_tasks[cache_key] = state
        state.task = asyncio.create_task(
            self._run_cache_task(cache_key, state, payload)
        )
        return state

    async def _poll_cache_record(
        self,
        group_id: str,
        message_id: str,
    ) -> dict[str, Any] | None:
        """撤回事件到达后轮询等待该消息的缓存结果。"""
        short_poll_interval = 0.1
        long_poll_interval = 1.0
        file_path = self._get_cache_file_path(group_id, message_id)
        cached_data = None
        started_at = time.monotonic()
        pending_deadline = time.monotonic() + self.settings.cache_pending_timeout
        logger.debug(
            f"[{self.instance_id}] 开始轮询缓存（群: {group_id}, 消息ID: {message_id}）"
        )

        while time.monotonic() < pending_deadline:
            cached_data = self._load_cached_data(file_path)
            if cached_data and str(cached_data.get("message_id", "")) == message_id:
                break
            await asyncio.sleep(short_poll_interval)
        else:
            return None

        if not cached_data:
            elapsed = time.monotonic() - started_at
            logger.warning(
                f"[{self.instance_id}] 缓存文件读取为空"
                f"（ID: {message_id}, 耗时: {elapsed:.2f}s）"
            )
            return None

        wait_deadline = started_at + self.settings.cache_expiration_time
        while time.monotonic() < wait_deadline:
            cached_data = self._load_cached_data(file_path)
            if not cached_data:
                await asyncio.sleep(long_poll_interval)
                continue
            status = cached_data.get("status")
            if status in ("done", "failed"):
                elapsed = time.monotonic() - started_at
                logger.debug(
                    f"[{self.instance_id}] 轮询结束"
                    f"（ID: {message_id}, 状态: {status}, 耗时: {elapsed:.2f}s）"
                )
                return cached_data
            await asyncio.sleep(long_poll_interval)

        elapsed = time.monotonic() - started_at
        logger.warning(
            f"[{self.instance_id}] 长轮询超时（ID: {message_id}, "
            f"上限: {self.settings.cache_expiration_time}s, 耗时: {elapsed:.2f}s）"
        )
        return None
