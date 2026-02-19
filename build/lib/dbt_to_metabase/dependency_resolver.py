"""
Resolve the execution order of dbt models using topological sort.

In a Metabase Transforms migration **all** dbt models -- including ephemeral
ones -- are materialized as tables.  Therefore the dependency resolver must
include ephemeral models in the execution order so that they are created before
downstream models that reference them.
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

    def resolve(self, exclude_seeds_only=False):
        # type: (bool) -> List[str]
        """Return a topologically-sorted list of model names.

        All model materializations (table, view, incremental, **ephemeral**)
        are included because Metabase transforms materialise everything as
        physical tables.

        If *exclude_seeds_only* is True, seed pseudo-models are removed from
        the result (they already exist as raw tables and don't need transforms).
        """
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

        if exclude_seeds_only:
            order = [
                m for m in order
                if not (self._model_by_name.get(m) and
                        self._model_by_name[m].config.get("is_seed"))
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
        """Return models grouped into parallelisable layers.

        All materializations are included (ephemeral models are materialised
        as transforms too).  Seeds are excluded because they are raw tables.
        """
        order = self.resolve(exclude_seeds_only=True)
        model_layer = {}  # type: Dict[str, int]

        for model_name in order:
            model = self._model_by_name.get(model_name)
            if not model:
                continue

            # Seeds are already excluded from *order* but guard anyway.
            if model.config.get("is_seed"):
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
