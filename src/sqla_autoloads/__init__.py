"""Automatic relationship loading for SQLAlchemy.

sqla_autoloads builds SELECT queries with eager-loaded relationships using
``sqla_select``.  Initialize a ``Node`` singleton at startup with your
declarative base, then call ``sqla_select(model=..., loads=(...))`` to get
a fully-joined query — LATERAL limits, self-referential handling, and
dotted-path traversal are all handled automatically.
"""

from ._version import __version__, __version_tuple__
from .core import SelectBuilder, sqla_select
from .datastructures import frozendict
from .node import Node, get_node, init_node
from .tools import add_conditions, get_primary_key, get_table_name, get_table_names


__all__ = (
    "Node",
    "SelectBuilder",
    "__version__",
    "__version_tuple__",
    "add_conditions",
    "frozendict",
    "get_node",
    "get_primary_key",
    "get_table_name",
    "get_table_names",
    "init_node",
    "sqla_select",
)
