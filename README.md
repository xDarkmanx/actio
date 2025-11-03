### Actio Project

Pure Python actor system for building concurrent and distributed applications with async/await support.

### Features

**Async/await native** - Built on Python 3.11+ asyncio

**Hierarchical actors** - Parent-child relationships with supervision

**Message routing** - Path-based message delivery between actors

**Lifecycle management** - Started/receive/stopped hooks with error handling

**Type safe** - Full type annotations with Pylance/MyPy support

### Quick Start

```python
import asyncio
from actio import Actor, ActorSystem, actio

@actio()
class MyActor(Actor):
    async def receive(self, sender, message):
        print(f"Received: {message}")

async def main():
    system = ActorSystem()
    actor_ref = system.create(MyActor())
    
    system.tell(actor_ref, "Hello World!")
    await asyncio.sleep(0.1)
    system.shutdown()

asyncio.run(main())
```

### License
MIT License - see LICENSE file for details.
