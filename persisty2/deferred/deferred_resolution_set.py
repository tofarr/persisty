from dataclasses import field, dataclass
from typing import List

from persisty.obj_graph.deferred.deferred_resolution_abc import DeferredResolutionABC


@dataclass(frozen=True)
class DeferredResolutionSet:
    deferred_resolutions: List[DeferredResolutionABC] = field(default_factory=list)

    def resolve(self):
        while True:
            state_changed = False
            for deferred_resolution in list(self.deferred_resolutions):
                state_changed = state_changed or deferred_resolution.resolve()
            if not state_changed:
                return
