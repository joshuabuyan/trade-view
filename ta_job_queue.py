import asyncio
import gc
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional


@dataclass
class TAJob:
    message: Any
    symbol: str
    context: Any
    market_type: str = "auto"
    chat_id: int = 0
    created_at: float = field(default_factory=time.time)


class TATaskQueue:
    """Minimal bounded queue for Technical Analysis jobs.

    Goal:
    - one active TA job at a time
    - tiny pending queue
    - no refactor of the TA core pipeline
    """

    def __init__(self, max_pending: int = 1, logger: Any = None):
        self.queue: asyncio.Queue[TAJob] = asyncio.Queue(maxsize=max(1, int(max_pending)))
        self.logger = logger
        self.worker_task: Optional[asyncio.Task] = None
        self.current_job: Optional[TAJob] = None
        self.current_task: Optional[asyncio.Task] = None
        self.runner: Optional[Callable[[Any, str, Any, str], Awaitable[Any]]] = None
        self._pending_chats: Dict[int, float] = {}

    async def start(self, runner: Callable[[Any, str, Any, str], Awaitable[Any]]) -> None:
        self.runner = runner
        if self.worker_task and not self.worker_task.done():
            return
        self.worker_task = asyncio.create_task(self._worker_loop(), name="ta_job_worker")

    async def stop(self) -> None:
        if self.current_task and not self.current_task.done():
            self.current_task.cancel()
            try:
                await self.current_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if self.worker_task and not self.worker_task.done():
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self.worker_task = None
        self.current_task = None
        self.current_job = None
        self._pending_chats.clear()
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except Exception:
                break
        gc.collect()

    def status(self) -> Dict[str, Any]:
        return {
            "running": self.current_job is not None,
            "queued": self.queue.qsize(),
            "max_pending": self.queue.maxsize,
        }

    async def enqueue(self, job: TAJob) -> Dict[str, Any]:
        if self.runner is None:
            raise RuntimeError("TA queue runner is not started")

        if self.current_job and self.current_job.chat_id == job.chat_id:
            return {"accepted": False, "reason": "chat_busy", "running": True, "position": 0}
        if job.chat_id in self._pending_chats:
            return {"accepted": False, "reason": "chat_already_queued", "running": self.current_job is not None, "position": 0}
        if self.queue.full():
            return {"accepted": False, "reason": "queue_full", "running": self.current_job is not None, "position": self.queue.qsize() + 1}

        self._pending_chats[job.chat_id] = time.time()
        await self.queue.put(job)
        return {
            "accepted": True,
            "running": self.current_job is not None,
            "position": self.queue.qsize(),
        }

    async def _worker_loop(self) -> None:
        while True:
            job = await self.queue.get()
            self.current_job = job
            self._pending_chats.pop(job.chat_id, None)
            try:
                gc.collect()
                if self.logger:
                    self.logger.info(
                        "[TA_QUEUE] starting symbol=%s market_type=%s chat_id=%s queued=%s",
                        job.symbol,
                        job.market_type,
                        job.chat_id,
                        self.queue.qsize(),
                    )
                self.current_task = asyncio.create_task(
                    self.runner(job.message, job.symbol, job.context, job.market_type),
                    name=f"ta_job_{job.symbol}_{job.chat_id}",
                )
                await self.current_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.logger:
                    self.logger.error(
                        "[TA_QUEUE] job failed symbol=%s market_type=%s chat_id=%s error=%s",
                        job.symbol,
                        job.market_type,
                        job.chat_id,
                        exc,
                        exc_info=True,
                    )
                try:
                    await job.message.reply_text(
                        "⚠️ Technical Analysis job failed. Please try again.",
                    )
                except Exception:
                    pass
            finally:
                self.current_task = None
                self.current_job = None
                self.queue.task_done()
                gc.collect()
                if self.logger:
                    self.logger.info("[TA_QUEUE] finished; queued=%s", self.queue.qsize())
