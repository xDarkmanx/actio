# actio/actor/mailbox.py
# -*- coding: utf-8 -*-
"""Mailbox wrapper for actor message queue."""

import asyncio

from typing import Any
from typing import Optional
from typing import Tuple

from ..ref import ActorRef

class Mailbox:
    """Asyncio-based mailbox for actor messages."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue = asyncio.Queue()
        self._closed: bool = False

    async def put(
        self,
        sender: Optional[ActorRef],
        message: Any
    ) -> None:
        """Put a message into the mailbox."""
        if self._closed:
            return
        await self._queue.put((sender, message))

    def put_nowait(
        self,
        sender: Optional[ActorRef],
        message: Any
    ) -> None:
        """Put a message into the mailbox without waiting."""
        if self._closed:
            return
        self._queue.put_nowait((sender, message))

    async def get(
        self
    ) -> Tuple[Optional[ActorRef], Any]:
        """Get a message from the mailbox."""
        return await self._queue.get()

    def get_nowait(
        self
    ) -> Tuple[Optional[ActorRef], Any]:
        """Get a message from the mailbox without waiting."""
        return self._queue.get_nowait()

    def empty(
        self
    ) -> bool:
        """Check if mailbox is empty."""
        return self._queue.empty()

    def qsize(
        self
    ) -> int:
        """Return the number of messages in the mailbox."""
        return self._queue.qsize()

    def close(
        self
    ) -> None:
        """Close the mailbox."""
        self._closed = True

    @property
    def is_closed(
        self
    ) -> bool:
        """Check if mailbox is closed."""
        return self._closed
