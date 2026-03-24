# actio/system/lifecycle.py
# -*- coding: utf-8 -*-
"""Actor lifecycle loop - extracted from ActorSystem for clarity."""
from __future__ import annotations

import asyncio
import logging

from typing import TYPE_CHECKING

from ..ref import ActorRef
from ..messages import PoisonPill
from ..messages import DeadLetter
from ..messages import Terminated

if TYPE_CHECKING:
    from .base import ActorSystem
    from ..actor.base import Actor
    from .base import _ActorContext

log = logging.getLogger("actio.system.lifecycle")


async def actor_lifecycle_loop(
    system: ActorSystem,
    actor: Actor,
    actor_ref: ActorRef,
    actor_ctx: _ActorContext
) -> None:
    """Main actor lifecycle loop.

    Args:
        system: ActorSystem instance
        actor: Actor instance
        actor_ref: ActorRef for this actor
        actor_ctx: ActorContext for this actor
    """
    try:
        await actor.started()
        actor_ctx.receiving_messages = True
    except Exception as e:
        log.exception(f"Exception raised while awaiting start of {actor_ref}", exc_info=e)

    while actor_ctx.receiving_messages:
        sender, message = await actor_ctx.mailbox.get()
        if isinstance(message, PoisonPill):
            break

        # Routing logic
        message_handled_by_routing = False
        if isinstance(message, dict) and message.get("action") == "route_message":
            try:
                if hasattr(actor, "_route_message_logic"):
                    message_handled_by_routing = await actor._route_message_logic(
                        sender, message
                    )
            except Exception as e:
                log.error(f"Error in _route_message_logic for {actor_ref.path}: {e}")

        if not message_handled_by_routing:
            try:
                await actor.receive(sender, message)
            except Exception as e:
                try:
                    await actor.restarted(sender, message, e)
                except Exception as e:
                    log.exception(f"Exception raised while awaiting restart of {actor_ref}", exc_info=e)

    actor_ctx.receiving_messages = False

    # Stop children
    children_stopping = []
    for child in actor_ctx.children:
        child_ctx = system._actors[child]
        # ✅ ИСПРАВЛЕНО: оборачиваем корутину в Task
        children_stopping.append(
            asyncio.create_task(child_ctx.is_stopped.wait())
        )
        system.stop(child)

    if children_stopping:
        # ✅ Теперь передаём Tasks, не coroutines
        await asyncio.wait(children_stopping)

    try:
        await actor.stopped()
    except Exception as e:
        log.exception(f"Exception raised while awaiting stop of {actor_ref}", exc_info=e)

    # Notify watchers
    for other in actor_ctx.watched_by:
        system._tell(other, Terminated(actor_ref), sender=None)
        other_ctx = system._actors[other]
        other_ctx.watching.remove(actor_ref)

    for other in actor_ctx.watching:
        other_ctx = system._actors[other]
        other_ctx.watched_by.remove(actor_ref)

    # Remove from parent
    if actor_ctx.parent:
        parent_ctx = system._actors[actor_ctx.parent]
        parent_ctx.children.remove(actor_ref)
    else:
        system._children.remove(actor_ref)

    # Clear mailbox
    while not actor_ctx.mailbox.empty():
        sender, message = actor_ctx.mailbox.get_nowait()
        if sender and sender != actor_ref:
            deadletter = DeadLetter(actor_ref, message)
            system._tell(sender, deadletter, sender=None)

    actor_ctx.is_stopped.set()
    del system._actors[actor_ref]

    try:
        del system._actor_names[actor_ref.name]
    except KeyError:
        pass
