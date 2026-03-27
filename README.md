# Actio Project

Pure Python actor system for building concurrent and distributed applications with async/await support.

## Features

**Async/await native** - Built on Python 3.11+ asyncio

**Hierarchical actors** - Parent-child relationships with supervision

**Message routing** - Path-based message delivery between actors

**Lifecycle management** - Started/receive/stopped hooks with error handling

**Type safe** - Full type annotations with Pylance/MyPy support

### Quick Start

```python
# test_standalone.py
# -*- coding: utf-8 -*-
"""Standalone mode test for actio library."""

import asyncio
import logging

from actio import Actor
from actio import ActorSystem
from actio import actio
from actio import ActorRef
from actio.registry import flush_pending_definitions
from actio.registry import get_default_registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

log = logging.getLogger("test_standalone")


@actio(name="TestActor", parent=None, replicas=1)
class TestActor(Actor):
    """Test root actor."""

    def __init__(self) -> None:
        super().__init__()
        self.message_count: int = 0

    async def started(self) -> None:
        log.info(f"✅ {self.name} started")

    async def receive(
        self,
        sender: ActorRef,
        message: any
    ) -> None:
        self.message_count += 1
        log.info(f"📨 {self.name} received #{self.message_count}: {message}")

        if isinstance(message, dict) and message.get("action") == "ping":
            # ✅ Проверка на None sender
            if sender is not None:
                self.tell(sender, {"action": "pong", "count": self.message_count})

    async def stopped(self) -> None:
        log.info(f"🛑 {self.name} stopped")

@actio(name="ChildActor", parent="TestActor", dynamic=True)
class ChildActor(Actor):
    """Test dynamic child actor."""

    async def started(self) -> None:
        log.info(f"✅ {self.name} started")

    async def receive(
        self,
        sender: ActorRef,
        message: any
    ) -> None:
        log.info(f"📨 {self.name} received: {message}")

    async def stopped(self) -> None:
        log.info(f"🛑 {self.name} stopped")


async def main() -> None:
    """Main test function."""
    log.info("🚀 Starting standalone test...")
    registry = get_default_registry()

    await flush_pending_definitions(registry)

    system = ActorSystem(registry=registry)

    await registry.build_actor_tree(system, timeout=5.0)
    root_ref = system.get_actor_ref_by_name("TestActor")
    if not root_ref:
        log.error("Failed to get TestActor reference")
        log.error(f"Available actors: {list(registry._definitions.keys())}")
        return

    log.info(f"Root actor created: {root_ref.path}")

    system.tell(root_ref, {"action": "ping"})
    system.tell(root_ref, {"action": "test", "data": "hello"})

    root_instance = system.get_actor_instance(root_ref)
    if root_instance:
        child_ref = await root_instance.create(ChildActor(), name="Child-1")
        log.info(f"✅ Child actor created: {child_ref.path}")

        system.tell(child_ref, {"action": "child_message"})

    await asyncio.sleep(1.0)

    registry.print_actor_tree()
    log.info("🛑 Shutting down...")
    await system.shutdown(timeout=5.0)

    log.info("✅ Standalone test complete!")


if __name__ == "__main__":
    asyncio.run(main())
```

### License

MIT License - see LICENSE file for details.
