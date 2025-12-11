# actio/cluster.py
# -*- coding: utf-8 -*

import logging
import random
import hashlib

from typing import Dict
from typing import Optional
from typing import List
from typing import Union

from . import ActorDefinition
from . import registry

log = logging.getLogger("actio.cluster")


class Crush:
    def __init__(self) -> None:
        self.nodes = {}
        self.virtual_nodes = 100

    def update_nodes(self, cluster_members):
        """Обновляет информацию о нодах кластера"""
        self.nodes = {
            node_id: self._calculate_weight(member_data)
            for node_id, member_data in cluster_members.items()
            if member_data.get("status") == "alive"
        }
        log.debug(f"CrushMapper updated nodes: {list(self.nodes.keys())}")

    def _calculate_weight(self, member_data):
        """Рассчитывает вес ноды на основе ресурсов и загрузки"""
        cpu_cores = member_data.get('resources', {}).get('cpu_cores', 4)
        memory_gb = member_data.get('resources', {}).get('memory_gb', 8)
        current_load = member_data.get('actor_count', 0)

        base_weight = (cpu_cores * 0.6 + memory_gb * 0.4)
        current_weight = base_weight / (current_load + 1)

        return max(current_weight, 0.1)

    def map_actors_to_nodes(self, actor_definitions: List[ActorDefinition]) -> Dict[str, List[tuple]]:
        """Распределяет акторы по нодам: {node_id: [(actor_name, replica_index)]}"""
        if not self.nodes:
            return {}

        placement = {}

        for defn in actor_definitions:
            # Получаем целевые ноды для этого актора
            target_nodes = self.map_actor(defn.name, defn.replicas)

            for replica_index, node_id in enumerate(target_nodes):
                if node_id not in placement:
                    placement[node_id] = []
                placement[node_id].append((defn.name, replica_index))

        return placement

    def map_actor(self, actor_name: str, replicas: Union[int, str] = 1) -> List[str]:
        """Распределяет реплики актора по нодам с учетом текущего состояния"""
        if not self.nodes:
            return []

        # 🔥 Обработка replicas='all'
        if replicas == 'all':
            target_nodes = list(self.nodes.keys())
            log.info(f"🎯 CrushMapper mapped {actor_name} to ALL nodes: {target_nodes}")
            return target_nodes

        # Преобразуем в int для обратной совместимости
        replica_count = int(replicas) if isinstance(replicas, str) else replicas

        current_replicas = registry.get_actor_replicas(actor_name)
        nodes_with_replicas = set(current_replicas.keys())
        available_nodes = list(self.nodes.keys())

        if not available_nodes:
            return []

        # 🔥 Single-replica с улучшенной балансировкой
        if replica_count == 1:
            return self._map_single_replica(actor_name, nodes_with_replicas, available_nodes)

        # 🔥 Multi-replica логика
        return self._map_multi_replica(actor_name, replica_count, nodes_with_replicas, available_nodes)

    def _map_single_replica(self, actor_name: str, nodes_with_replicas: set, available_nodes: List[str]) -> List[str]:
        """Распределение single-replica акторов с балансировкой"""
        # Стратегия 1: Если есть текущая реплика и нода жива - оставляем на ней
        if nodes_with_replicas:
            current_node = next(iter(nodes_with_replicas))
            if current_node in available_nodes:
                log.info(f"🎯 CrushMapper keeping {actor_name} on current node: {current_node}")
                return [current_node]

        # Стратегия 2: Round-robin распределение с учетом загрузки
        available_nodes.sort()  # Для детерминированности
        actor_hash = hash(actor_name) % len(available_nodes)
        selected_node = available_nodes[actor_hash]

        # Стратегия 3: Если выбранная нода перегружена - найти менее загруженную
        selected_weight = self.nodes.get(selected_node, 1.0)
        if selected_weight < 0.5:  # Нода перегружена
            best_node = max(available_nodes, key=lambda n: self.nodes.get(n, 1.0))
            log.info(f"🔄 CrushMapper rebalanced {actor_name} from {selected_node} to {best_node} (load balancing)")
            return [best_node]

        log.info(f"🎯 CrushMapper round-robin mapped {actor_name} to node: {selected_node}")
        return [selected_node]

    def _map_multi_replica(
        self,
        actor_name: str,
        replica_count: int,
        nodes_with_replicas: set,
        available_nodes: List[str]
    ) -> List[str]:
        """Распределение multi-replica акторов"""
        actor_hash = int(hashlib.md5(actor_name.encode()).hexdigest()[:8], 16)
        placement = []

        # Сначала добавляем ноды которые уже имеют реплики (если нужно сохранить их)
        for node in list(nodes_with_replicas):
            if len(placement) < replica_count and node in available_nodes:
                placement.append(node)
                available_nodes.remove(node)
                log.debug(f"🔁 CrushMapper keeping existing replica {actor_name} on node: {node}")

        # Добавляем новые ноды если нужно больше реплик
        while len(placement) < replica_count and available_nodes:
            selected_node = self._weighted_selection(available_nodes, actor_hash + len(placement))
            if selected_node:
                placement.append(selected_node)
                available_nodes.remove(selected_node)
                log.debug(f"➕ CrushMapper adding new replica {actor_name} on node: {selected_node}")
            else:
                break

        # Если все еще не хватает реплик - пытаемся использовать уже занятые ноды
        if len(placement) < replica_count:
            all_occupied_nodes = list(nodes_with_replicas) + placement
            unique_occupied_nodes = list(set(all_occupied_nodes))

            for node in unique_occupied_nodes:
                if len(placement) < replica_count and node not in placement:
                    placement.append(node)
                    log.debug(f"🔄 CrushMapper reusing node {node} for {actor_name}")

        log.info(
            f"🎯 CrushMapper mapped {actor_name} to nodes: {placement} "
            f"(requested: {replica_count}, available: {list(self.nodes.keys())})"
        )
        return placement

    def _weighted_selection(self, available_nodes: List[str], seed: int) -> Optional[str]:
        """Взвешенный выбор ноды на основе весов"""
        if not available_nodes:
            return None

        # Создаем взвешенный список
        weighted_nodes = []
        for node in available_nodes:
            weight = self.nodes.get(node, 1.0)
            # Добавляем ноду несколько раз в зависимости от веса
            count = max(1, int(weight * 10))
            weighted_nodes.extend([node] * count)

        if not weighted_nodes:
            return None

        # Детерминированный выбор на основе seed
        random.seed(seed)
        selected = random.choice(weighted_nodes)
        random.seed()  # Сбрасываем seed

        log.debug(
            f"🎲 Weighted selection: {selected} from {available_nodes} "
            f"(weights: {[self.nodes.get(n, 1.0) for n in available_nodes]})"
        )
        return selected

    def get_optimal_node_for_actor(self, actor_name: str) -> Optional[str]:
        """Возвращает оптимальную ноду для нового актора"""
        if not self.nodes:
            return None

        available_nodes = list(self.nodes.keys())
        if not available_nodes:
            return None

        # Выбираем ноду с максимальным весом (наименее загруженную)
        best_node = max(available_nodes, key=lambda n: self.nodes.get(n, 1.0))
        log.debug(
            f"🏆 Optimal node for {actor_name}: {best_node} "
            f"(weight: {self.nodes.get(best_node, 1.0)})"
        )
        return best_node

    def get_node_load(self, node_id: str) -> float:
        """Возвращает текущую нагрузку ноды (обратный вес)"""
        weight = self.nodes.get(node_id, 1.0)
        return 1.0 / weight if weight > 0 else float('inf')

    def print_node_weights(self):
        """Логирует текущие веса нод (для отладки)"""
        if not self.nodes:
            log.info("📊 No nodes available in CrushMapper")
            return

        log.info("📊 CrushMapper node weights:")
        for node_id, weight in sorted(self.nodes.items(), key=lambda x: x[1], reverse=True):
            load = 1.0 / weight if weight > 0 else float('inf')
            log.info(f"   {node_id}: weight={weight:.2f}, load={load:.2f}")
