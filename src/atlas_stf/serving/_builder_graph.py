"""Graph materialization: nodes and edges from existing serving tables.

Re-export hub — implementation split across:
- _builder_graph_nodes.py  (ID helpers, model constructors, node/edge builders)
- _builder_graph_meta.py   (module availability, path candidates, bundles, review queue, orchestrator)
"""

from __future__ import annotations

from ._builder_graph_meta import (
    _MODULE_CHECKS,
    _build_evidence_bundles,
    _build_module_availability,
    _build_path_candidates,
    _build_review_queue,
    materialize_graph,
)
from ._builder_graph_nodes import (
    _BATCH,
    _J,
    _bid,
    _build_edges,
    _build_nodes,
    _eid,
    _nid,
    _pid,
)

__all__ = [
    "_BATCH",
    "_J",
    "_MODULE_CHECKS",
    "_bid",
    "_build_edges",
    "_build_evidence_bundles",
    "_build_module_availability",
    "_build_nodes",
    "_build_path_candidates",
    "_build_review_queue",
    "_eid",
    "_nid",
    "_pid",
    "materialize_graph",
]
