# actio/registry/decorator.py
# -*- coding: utf-8 -*-
"""@actio decorator for actor class registration."""

import logging

from typing import Dict
from typing import Optional
from typing import Any
from typing import Union
from typing import Callable
from typing import Type
from typing import List

from ..ref import ActorDefinition

log = logging.getLogger("actio.registry.decorator")

# Глобальный список отложенных определений
_pending_definitions: List[ActorDefinition] = []

def actio(
    name: Optional[str] = None,
    parent: Optional[str] = None,
    replicas: Union[int, str] = 1,
    minimal: int = 1,
    weight: float = 0.01,
    dynamic: bool = False,
    config: Optional[Dict[str, Any]] = None
) -> Callable[[Type], Type]:
    """Decorator to register an actor class."""

    def decorator(cls: Type) -> Type:
        actor_name = name or cls.__name__

        if isinstance(replicas, str) and replicas != 'all':
            raise ValueError(
                f"Invalid replicas value: {replicas}. Must be integer or 'all'"
            )

        definition = ActorDefinition(
            name=actor_name,
            cls=cls,
            parent=parent,
            replicas=replicas,
            minimal=minimal,
            weight=weight,
            dynamic=dynamic,
            config=config or {}
        )

        # ✅ Просто сохраняем в глобальный список (синхронно, без asyncio)
        _pending_definitions.append(definition)

        log.debug(f"Decorated actor class: {actor_name}")
        return cls

    return decorator


async def flush_pending_definitions(registry: Any) -> None:
    """Register all pending definitions with the given registry.

    Call this before build_actor_tree().

    Args:
        registry: Registry instance to register definitions with
    """
    if not _pending_definitions:
        return

    for definition in _pending_definitions:
        await registry.register_definition(definition)
        log.info(f"Registered definition: {definition.name}")

    _pending_definitions.clear()


def get_pending_definitions() -> List[ActorDefinition]:
    """Get list of pending definitions (for debugging)."""
    return _pending_definitions.copy()
