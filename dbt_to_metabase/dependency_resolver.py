"""
Resolve the execution order of dbt models using topological sort.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set

from .models import DbtModel, DbtProject

logger = logging.getLogger(__name__)


class CyclicDependencyError(Exception):
    pass


class DependencyResolver:
    def __init__(self, project):
        # type: (DbtProject) -> None
        self.project = project
        self._model_by_name = {
            m.name: m for m in project.models.values()
        }  # type: Dict[str, DbtModel]

    def resolve(self, exclude_ephemeral=True):
        # type: (bool) -> List[str]
        graph = defaultdict(set)  # type: Dict[str, Set[str]]
        all_models = set()  # type: Set[str]

        for model in self.project.models.values():
            all_models.add(model.name)
            for dep_name in model.depends_on_models:
                if dep_name in self._model_by_name:
                    graph[model.name].add(dep_name)

        in_degree = {m: 0 for m in all_models}  # type: Dict[str, int]
        reverse = defaultdict(set)  # type: Dict[str, Set[str]]

        for model_name, deps in graph.items():
            for dep in deps:
                in_degree[model_name] = in_degree.get(model_name, 0)
                if dep in all_models:
                    in_degree[model_name] += 1
                    reverse[dep].add(model_name)

        queue = deque()  # type: deque
        for m in all_models:
            if in_degree.get(m, 0) == 0:
                queue.append(m)

        order = []  # type: List[str]
        while queue:
            current = queue.popleft()
            order.append(current)
            for dependent in sorted(reverse.get(current, [])):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(order) != len(all_models):
            remaining = all_models - set(order)
            raise CyclicDependencyError(
                "Cyclic dependency detected among models: {}".format(remaining)
            )

        if exclude_ephemeral:
            order = [
                m
                for m in order
                if not self._model_by_name.get(m, DbtModel(
                    unique_id="", name=m, path="", raw_sql=""
                )).is_ephemeral
            ]

        return order

    def get_upstream(self, model_name):
        # type: (str) -> List[str]
        visited = set()  # type: Set[str]
        order = []  # type: List[str]

        def dfs(name):
            # type: (str) -> None
            if name in visited:
                return
            visited.add(name)
            model = self._model_by_name.get(name)
            if model:
                for dep in model.depends_on_models:
                    dfs(dep)
            order.append(name)

        dfs(model_name)
        return order[:-1]

    def get_downstream(self, model_name):
        # type: (str) -> List[str]
        downstream = []  # type: List[str]
        for model in self.project.models.values():
            if model_name in model.depends_on_models:
                downstream.append(model.name)
        return downstream

    def get_execution_layers(self):
        # type: () -> List[List[str]]
        order = self.resolve(exclude_ephemeral=True)
        model_layer = {}  # type: Dict[str, int]

        for model_name in order:
            model = self._model_by_name.get(model_name)
            if not model:
                continue
            deps = [
                d for d in model.depends_on_models
                if d in model_layer
            ]
            if deps:
                layer = max(model_layer[d] for d in deps) + 1
            else:
                layer = 0
            model_layer[model_name] = layer

        layers = defaultdict(list)  # type: Dict[int, List[str]]
        for name, layer in model_layer.items():
            layers[layer].append(name)

        if layers:
            return [layers[i] for i in range(max(layers.keys()) + 1)]
        return []
