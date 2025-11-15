# actio/actor.py
# -*- coding: utf-8 -*-

from typing import Union
from typing import Set
from typing import Dict
from typing import List
from typing import Any
from typing import Optional

import asyncio
import logging

from uuid import uuid4

from . import PoisonPill
from . import DeadLetter
from . import Terminated
from . import ActorRef

log = logging.getLogger('actio.actor')


class Actor:
    def __init__(self):
        self._context: Optional['_ActorContext'] = None

    async def receive(self, sender: ActorRef, message: Any) -> None:
        """
        Обрабатывает входящие сообщения.

        Этот метод должен быть переопределён в подклассах.
        """
        raise NotImplementedError

    def create(self, actor: 'Actor', name: Optional[str] = None) -> ActorRef:
        """Создаёт дочерний актор."""
        # УСТАНАВЛИВАЕМ _definition ПЕРЕД созданием
        self._set_actor_definition(actor)

        child_actor_ref = self.system._create(actor=actor, parent=self.actor_ref, name=name)
        self.watch(child_actor_ref)
        return child_actor_ref

    def _set_actor_definition(self, actor: 'Actor'):
        """Устанавливает _definition для актора из registry."""
        try:
            from . import registry
            actor_class = type(actor)

            # Ищем определение в registry
            for defn in registry._definitions.values():
                if defn.cls == actor_class:
                    actor._definition = defn
                    return

            for defn in registry._dynamic_definitions.values():
                if defn.cls == actor_class:
                    actor._definition = defn
                    return

            log.debug(f"No definition found for actor class: {actor_class.__name__}")
        except ImportError:
            log.debug("Registry not available for setting actor definition")

    def tell(self, actor: Union['Actor', ActorRef], message: Any) -> None:
        """Отправляет сообщение другому актору."""
        self.system._tell(actor=actor, message=message, sender=self.actor_ref)

    def watch(self, actor: ActorRef) -> None:
        """Наблюдает за другим актором."""
        self.system._watch(actor=self.actor_ref, other=actor)

    def unwatch(self, actor: ActorRef) -> None:
        """Прекращает наблюдение за другим актором."""
        self.system._unwatch(actor=self.actor_ref, other=actor)

    def stop(self) -> None:
        """Останавливает этот актор."""
        self.system.stop(actor=self.actor_ref)

    # --- Хуки жизненного цикла ---
    async def started(self) -> None:
        """Вызывается при запуске актора."""
        pass

    async def restarted(self, sender: ActorRef, message: Any, error: Exception) -> None:
        """Вызывается при ошибке в `receive`."""
        log.exception(
            '%s failed to receive message %s from %s',
            self.actor_ref, message, sender, exc_info=error
        )

    async def stopped(self) -> None:
        """Вызывается при остановке актора."""
        pass

    # --- Свойства для доступа к контексту ---
    @property
    def actor_ref(self) -> ActorRef:
        assert self._context is not None, "Actor context not initialized"
        return self._context.actor_ref

    @property
    def system(self) -> 'ActorSystem':
        assert self._context is not None, "Actor context not initialized"
        return self._context.system

    @property
    def parent(self) -> Optional[ActorRef]:
        assert self._context is not None, "Actor context not initialized"
        return self._context.parent

    @property
    def name(self) -> str:
        return self.actor_ref.name

    @property
    def path(self) -> str:
        return self.actor_ref.path

    def __str__(self):
        return self.path

    def __repr__(self):
        return self.path

    # --- Вспомогательные методы для маршрутизации ---
    async def _route_message_logic(self, sender: ActorRef, message: Dict[str, Any]) -> bool:
        """
        Встроенная логика маршрутизации для сообщений с действием 'route_message'.

        :param sender: Отправитель сообщения.
        :param message: Сообщение.
        :return: True, если сообщение было обработано как маршрутное, иначе False.
        """
        action = message.get('action')
        if action != 'route_message':
            return False

        destination: str = message.get('destination', '')
        data = message.get('data')

        # 1. Если destination пустой, это сообщение для текущего актора
        if not destination:
            final_message = data if isinstance(data, dict) else {'data': data}
            final_message['source'] = message.get('source')
            await self.receive(sender, final_message)
            return True

        # 2. Разбираем путь
        path_parts = [p for p in destination.split('/') if p]
        if not path_parts:
            final_message = data if isinstance(data, dict) else {'data': data}
            await self.receive(sender, final_message)
            return True

        first_part = path_parts[0]
        remaining_path = "/".join(path_parts[1:]) if len(path_parts) > 1 else None

        # 3. Ищем ребенка
        target_child_ref: Optional[ActorRef] = None
        child_actors_dict = getattr(self, 'actors', None)
        if child_actors_dict and isinstance(child_actors_dict, dict):
            target_child_ref = child_actors_dict.get(first_part)
        else:
            assert self._context is not None, "Actor context not initialized"
            for child_ref in self._context.children:
                if child_ref.name == first_part:
                    target_child_ref = child_ref
                    break

        if target_child_ref:
            # Нашли потомка - пересылаем ему
            forwarded_message = message.copy()
            forwarded_message['destination'] = remaining_path
            current_source = forwarded_message.get('source', '')

            log.debug(
                f"[{self.actor_ref.path}] Routing to child '{first_part}' "
                f"with new destination '{remaining_path}'"
            )
            self.tell(target_child_ref, forwarded_message)
        else:
            # Не нашли - пересылаем родителю
            if self.parent:
                forwarded_message = message.copy()
                current_source = forwarded_message.get('source', '')
                forwarded_message['source'] = f"{self.actor_ref.name}/{current_source}".strip('/')

                log.debug(
                    f"[{self.actor_ref.path}] Forwarding to parent"
                )
                self.tell(self.parent, forwarded_message)
            else:
                log.warning(
                    f"[{self.actor_ref.path}] Cannot route message, "
                    f"no parent and '{first_part}' not found among children"
                )

        return True


class ActorSystem:
    """Система акторов."""
    def __init__(self):
        self._actors: Dict[ActorRef, '_ActorContext'] = {}
        self._is_stopped = asyncio.Event()
        self.children: List[ActorRef] = []

    def create(self, actor: Actor, name: Optional[str] = None) -> ActorRef:
        """Создаёт корневой актор."""
        return self._create(actor=actor, parent=None, name=name)

    def tell(self, actor: Union[Actor, ActorRef], message: Any) -> None:
        """Отправляет сообщение актору."""
        self._tell(actor=actor, message=message, sender=None)

    def stop(self, actor: Union[Actor, ActorRef]) -> None:
        """Останавливает актор."""
        self._tell(actor=actor, message=PoisonPill(), sender=None)

    def shutdown(self, timeout: Union[None, int, float] = None) -> None:
        """Завершает работу всей системы акторов."""
        asyncio.create_task(self._shutdown(timeout=timeout))

    def stopped(self):
        """Ожидает завершения работы системы."""
        return self._is_stopped.wait()

    # --- Внутренние методы ---
    def _create(self, actor: Actor, *, parent: Union[None, Actor, ActorRef], name: Optional[str] = None) -> ActorRef:
        """Внутренний метод создания актора."""
        if not isinstance(actor, Actor):
            raise ValueError(f'Not an actor: {actor}')

        parent_ctx: Optional[_ActorContext] = None
        if parent:
            parent = self._validate_actor_ref(parent)
            parent_ctx = self._actors[parent]
            child_idx = len(parent_ctx.children) + 1
        else:
            child_idx = len(self.children) + 1

        if not name:
            name = f'{type(actor).__name__}-{child_idx}'

        if parent:
            path = f'{parent.path}/{name}'
        else:
            path = name

        actor_id = str(uuid4().hex)
        actor_ref = ActorRef(actor_id=actor_id, path=path, name=name)
        actor_ctx = _ActorContext(self, actor_ref, parent)

        actor_ctx.lifecycle = asyncio.get_event_loop().create_task(
            self._actor_lifecycle_loop(actor, actor_ref, actor_ctx)
        )

        actor._context = actor_ctx
        self._actors[actor_ref] = actor_ctx

        if parent and parent_ctx:
            parent_ctx.children.append(actor_ref)
        else:
            self.children.append(actor_ref)

        if hasattr(actor, '_definition') and getattr(actor._definition, 'dynamic', False):
            try:
                from . import registry
                registry.register_instance(actor._definition.name, actor_ref)

            except ImportError as e:
                log.error(f"Failed to register dynamic instance: {e}")

        return actor_ref

    def _tell(self, actor: Union[Actor, ActorRef], message: Any, *, sender: Union[None, Actor, ActorRef]) -> None:
        """Внутренний метод отправки сообщения."""
        actor = self._validate_actor_ref(actor)

        if sender:
            sender = self._validate_actor_ref(sender)
            if sender not in self._actors:
                raise ValueError(f'Sender does not exist: {sender}')

        if actor in self._actors:
            actor_ctx = self._actors[actor]
            actor_ctx.letterbox.put_nowait((sender, message))
        elif sender:
            deadletter = DeadLetter(actor=actor, message=message)
            self._tell(sender, deadletter, sender=None)
        else:
            log.warning(f'Failed to deliver message {message} to {actor}')

    def _watch(self, actor: Union[Actor, ActorRef], other: Union[Actor, ActorRef]) -> None:
        """Внутренний метод для наблюдения за актором."""
        actor = self._validate_actor_ref(actor)
        if actor not in self._actors:
            raise ValueError(f'Actor does not exist: {actor}')

        other = self._validate_actor_ref(other)
        if other not in self._actors:
            raise ValueError(f'Actor does not exist: {other}')

        if actor == other:
            raise ValueError(f'Actor cannot watch themselves: {actor}')

        actor_ctx = self._actors[actor]
        other_ctx = self._actors[other]
        actor_ctx.watching.add(other)
        other_ctx.watched_by.add(actor)

    def _unwatch(self, actor: Union[Actor, ActorRef], other: Union[Actor, ActorRef]) -> None:
        """Внутренний метод для прекращения наблюдения за актором."""
        actor = self._validate_actor_ref(actor)
        if actor not in self._actors:
            raise ValueError(f'Actor does not exist: {actor}')

        other = self._validate_actor_ref(other)
        if actor == other:
            raise ValueError(f'Actor cannot unwatch themselves: {actor}')

        actor_ctx = self._actors[actor]
        if other in actor_ctx.watching:
            actor_ctx.watching.remove(other)

        if other in self._actors:
            other_ctx = self._actors[other]
            if actor in other_ctx.watched_by:
                other_ctx.watched_by.remove(actor)

    async def _shutdown(self, timeout: Union[None, int, float] = None) -> None:
        """Внутренний метод завершения работы системы."""
        if self._actors:
            for actor_ref in self.children:
                self.stop(actor_ref)

            lifecycle_tasks = [
                task for task
                in (actor_ctx.lifecycle for actor_ctx in self._actors.values())
                if task is not None
            ]

            done, pending = await asyncio.wait(lifecycle_tasks, timeout=timeout)
            for lifecycle_task in pending:
                lifecycle_task.cancel()

        self._is_stopped.set()

    async def _actor_lifecycle_loop(self, actor: Actor, actor_ref: ActorRef, actor_ctx: '_ActorContext') -> None:
        """Основной цикл жизни актора."""
        try:
            await actor.started()
            actor_ctx.receiving_messages = True
        except Exception as e:
            log.exception('Exception raised while awaiting start of %s', actor_ref, exc_info=e)

        while actor_ctx.receiving_messages:
            sender, message = await actor_ctx.letterbox.get()
            if isinstance(message, PoisonPill):
                break

            # Логика маршрутизации
            message_handled_by_routing = False
            if isinstance(message, dict) and message.get('action') == 'route_message':
                try:
                    if hasattr(actor, '_route_message_logic'):
                        message_handled_by_routing = await actor._route_message_logic(sender, message)
                except Exception as e:
                    log.error(f"Error in _route_message_logic for {actor_ref.path}: {e}")

            if not message_handled_by_routing:
                try:
                    await actor.receive(sender, message)
                except Exception as e:
                    try:
                        await actor.restarted(sender, message, e)
                    except Exception as e:
                        log.exception(f'Exception raised while awaiting restart of {actor_ref}', exc_info=e)

        actor_ctx.receiving_messages = False

        # Остановка детей
        children_stopping = []
        for child in actor_ctx.children:
            child_ctx = self._actors[child]
            children_stopping.append(child_ctx.is_stopped.wait())
            self.stop(child)

        if children_stopping:
            await asyncio.wait(children_stopping)

        try:
            await actor.stopped()
        except Exception as e:
            log.exception(f'Exception raised while awaiting stop of {actor_ref}', exc_info=e)

        # Уведомление наблюдателей
        for other in actor_ctx.watched_by:
            self._tell(other, Terminated(actor_ref), sender=None)
            other_ctx = self._actors[other]
            other_ctx.watching.remove(actor_ref)

        for other in actor_ctx.watching:
            other_ctx = self._actors[other]
            other_ctx.watched_by.remove(actor_ref)

        # Удаление из родителя
        if actor_ctx.parent:
            parent_ctx = self._actors[actor_ctx.parent]
            parent_ctx.children.remove(actor_ref)
        else:
            self.children.remove(actor_ref)

        # Очистка очереди
        while not actor_ctx.letterbox.empty():
            sender, message = actor_ctx.letterbox.get_nowait()
            if sender and sender != actor_ref:
                deadletter = DeadLetter(actor_ref, message)
                self._tell(sender, deadletter, sender=None)

        actor_ctx.is_stopped.set()
        del self._actors[actor_ref]

    @staticmethod
    def _validate_actor_ref(actor: Union[Actor, ActorRef]) -> ActorRef:
        """Проверяет и возвращает ActorRef."""
        if isinstance(actor, Actor):
            actor = actor.actor_ref

        if not isinstance(actor, ActorRef):
            raise ValueError(f'Not an actor: {actor}')

        return actor


class _ActorContext:
    """Контекст выполнения актора."""
    def __init__(self, system: ActorSystem, actor_ref: ActorRef, parent: Optional[ActorRef]):
        self.system: ActorSystem = system
        self.actor_ref: ActorRef = actor_ref
        self.parent: Optional[ActorRef] = parent
        self.letterbox: asyncio.Queue = asyncio.Queue()
        self.lifecycle: Optional[asyncio.Task] = None
        self.watching: Set[ActorRef] = set()
        self.watched_by: Set[ActorRef] = set()
        self.children: List[ActorRef] = []
        self.is_stopped: asyncio.Event = asyncio.Event()
        self.receiving_messages: bool = False
