from __future__ import annotations

import sys
import warnings
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, TypeVar


if sys.version_info >= (3, 11):
    from typing import Required, TypedDict, Unpack
else:
    from typing_extensions import Required, TypedDict, Unpack

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.util import LoaderCriteriaOption
from sqlalchemy.sql.selectable import LateralFromClause
from sqlalchemy.sql.util import ClauseAdapter

from .datastructures import frozendict
from .node import Node
from .tools import get_primary_key, get_table_name, get_table_names


if TYPE_CHECKING:
    from sqlalchemy.orm.strategy_options import _AbstractLoad

T = TypeVar("T", bound=orm.DeclarativeBase)
DEFAULT_RELATIONSHIP_LOAD_LIMIT: Final[int] = 50

_MANY_LOAD_STRATEGIES: Final[dict[str, Callable[..., _AbstractLoad]]] = {
    "subqueryload": orm.subqueryload,
    "selectinload": orm.selectinload,
}
_ManyLoadStrategy = Literal["subqueryload", "selectinload"]


@dataclass(slots=True)
class _LoadSelfParams:
    relationship: orm.RelationshipProperty[orm.DeclarativeBase]
    load: _AbstractLoad | None = None


@dataclass(slots=True)
class _LoadRelationParams:
    relationship: orm.RelationshipProperty[orm.DeclarativeBase]
    is_alias: bool = False
    load: _AbstractLoad | None = None
    rn_series: sa.Subquery | None = None


@dataclass(slots=True, frozen=True)
class _LoadParams(Generic[T]):
    __class_getitem__ = classmethod(lambda cls, *args: cls)

    model: type[T]
    loads: tuple[str, ...] = ()
    node: Node = field(default_factory=Node)
    limit: int | None = field(default=DEFAULT_RELATIONSHIP_LOAD_LIMIT)
    check_tables: bool = field(default=False)
    distinct: bool = field(default=False)
    conditions: (
        Mapping[
            str,
            Callable[[sa.Select[tuple[T]]], sa.Select[tuple[T]]],
        ]
        | None
    ) = field(default=None)
    self_key: str = field(default="")
    order_by: tuple[str, ...] | None = field(default=None)
    query: sa.Select[tuple[T]] | None = field(default=None)
    many_load: _ManyLoadStrategy = field(default="subqueryload")
    optimization: bool = field(default=True)


class _LoadParamsType(TypedDict, Generic[T], total=False):
    model: Required[type[T]]
    loads: tuple[str, ...]
    node: Node
    check_tables: bool
    conditions: Mapping[str, Callable[[sa.Select[tuple[T]]], sa.Select[tuple[T]]]]
    self_key: str
    order_by: tuple[str, ...]
    query: sa.Select[tuple[T]]
    distinct: bool
    limit: int | None
    many_load: _ManyLoadStrategy
    optimization: bool


class SelectBuilder(Generic[T]):
    """Query builder that orchestrates join construction and eager-loading strategy selection.

    Constructs a ``sa.Select`` with the appropriate outer-joins, LATERAL sub-queries,
    ``contains_eager`` / ``selectinload`` / ``joinedload`` options, and optional
    ``WHERE`` conditions for each relationship path requested via ``build()``.

    One instance is created per unique set of load parameters (model + options);
    ``_select_with_relationships`` caches the resulting query via ``@lru_cache``.
    """

    __slots__ = (
        "_first_load_by_class",
        "_lateral_map",
        "_loaded",
        "_options",
        "_query",
        "_seen_classes",
        "_self_ref_loaded",
        "_zip_levels",
        "check_tables",
        "conditions",
        "distinct",
        "limit",
        "many_load",
        "model",
        "node",
        "optimization",
        "order_by",
        "self_key",
    )

    def __init__(
        self,
        model: type[T],
        node: Node,
        *,
        limit: int | None,
        check_tables: bool,
        conditions: Mapping[
            str,
            Callable[[sa.Select[tuple[T]]], sa.Select[tuple[T]]],
        ]
        | None,
        self_key: str,
        order_by: tuple[str, ...] | None,
        many_load: str,
        distinct: bool,
        optimization: bool = True,
    ) -> None:
        if orm.DeclarativeBase in getattr(model, "__bases__", ()) or model is orm.DeclarativeBase:
            raise TypeError("model must not be orm.DeclarativeBase")

        self.model = model
        self.node = node
        self.limit = limit
        self.check_tables = check_tables
        self.conditions = conditions
        self.self_key = self_key
        self.order_by = order_by
        self.many_load = many_load
        self.distinct = distinct
        self.optimization = optimization
        self._query: sa.Select[tuple[T]] = sa.select(model)
        self._options: list[_AbstractLoad | LoaderCriteriaOption] = []
        self._loaded: dict[str, _AbstractLoad] = {}
        self._seen_classes: set[type] = {model}
        self._self_ref_loaded: set[type] = set()
        self._first_load_by_class: dict[type, _AbstractLoad] = {}
        self._lateral_map: dict[sa.Table | sa.FromClause, LateralFromClause] = {}
        self._zip_levels: dict[int, sa.Subquery] = {}

    def build(
        self,
        loads: tuple[str, ...] = (),
        query: sa.Select[tuple[T]] | None = None,
    ) -> sa.Select[tuple[T]]:
        """Build the final SELECT with all requested relationship loads.

        Dotted paths (e.g. ``"posts.comments"``) are sorted deepest-first so that
        inner joins/options are registered before shallower ones reference them.
        Simple (non-dotted) names are appended after all dotted paths.

        Args:
            loads: Relationship key paths to eager-load.
            query: Optional pre-existing SELECT to extend instead of ``sa.select(model)``.

        Returns:
            Fully configured ``sa.Select`` with joins and eager-load options applied.
        """
        if query is not None:
            self._query = query

        # Sort: dotted paths first (deeper = more dots = processed first),
        # then simple names.
        dotted = sorted(
            [load for load in loads if "." in load],
            key=lambda x: x.count("."),
            reverse=True,
        )
        simple = [load for load in loads if "." not in load]

        # Resolve all loads before processing so we can reorder when needed.
        resolved: list[Sequence[orm.RelationshipProperty[orm.DeclarativeBase]]] = []
        for load_key in (*dotted, *simple):
            if "." in load_key:
                result = _resolve_dotted_path(self.model, load_key, self.node)
            else:
                result = _bfs_search(self.model, load_key, self.node)

            if result:
                resolved.append(result)

        self._construct(resolved)

        if self._options:
            self._query = self._query.options(*self._options)

        return self._query.distinct() if self.distinct else self._query

    def _construct_loads(
        self,
        relationships: Sequence[orm.RelationshipProperty[orm.DeclarativeBase]],
    ) -> list[_AbstractLoad | LoaderCriteriaOption] | None:
        """Walk a chain of relationships and build chained loader options.

        For each hop in the chain, determines whether the relationship was already
        loaded (reuses existing option), or delegates to ``_load_relationship`` for
        a fresh join/strategy.  Conditions are applied in three cases:

        1. O2M with ``limit=None`` (no LATERAL — condition goes on the subquery).
        2. A deeper dotted path re-visiting a previously loaded key.
        3. Self-referential M2O (``model is relation_cls``).

        Returns:
            List of loader options + criteria, or ``None`` if the chain is empty.
        """
        if not relationships:
            return None

        load: _AbstractLoad | None = None
        load_criteria: list[LoaderCriteriaOption] = []
        extra_options: list[_AbstractLoad] = []
        cumulative_path = ""

        for depth, relationship in enumerate(relationships):
            relation_cls = relationship.mapper.class_
            key = relationship.key
            cumulative_path = f"{cumulative_path}.{key}" if cumulative_path else key
            is_self_ref = relation_cls is self.model and relationship.parent.class_ is relation_cls

            if (
                self.conditions
                and (condition := self.conditions.get(key))
                and not is_self_ref
                and (
                    (relationship.uselist and self.limit is None)
                    or (cumulative_path in self._loaded and key not in self._loaded)
                )
            ) and (clause := condition(sa.select(relation_cls)).whereclause) is not None:
                load_criteria.append(orm.with_loader_criteria(relation_cls, clause))

            if cumulative_path in self._loaded:
                load = self._loaded[cumulative_path]
                self._seen_classes.add(relation_cls)
                continue

            is_alias = relation_cls in self._seen_classes
            rn_series = self._zip_levels.get(depth) if relationship.uselist else None

            self._query, load = self._load_relationship(
                _LoadRelationParams(
                    relationship=relationship,
                    load=load,
                    is_alias=is_alias,
                    rn_series=rn_series,
                ),
            )
            self._loaded[cumulative_path] = load

            # For is_alias M2O: also load from the first occurrence of the parent
            # entity class. Chained selectinload through identity-map-resident
            # objects can't populate attributes set by noload; loading from the
            # first occurrence bypasses this.
            if is_alias and not relationship.uselist:
                parent_cls = relationship.parent.class_
                if (first_load := self._first_load_by_class.get(parent_cls)) is not None:
                    extra_options.append(
                        _construct_strategy(orm.selectinload, relationship, first_load)
                    )

            if relation_cls not in self._first_load_by_class:
                self._first_load_by_class[relation_cls] = load

            self._seen_classes.add(relation_cls)

        return [load, *load_criteria, *extra_options] if load else None

    def _load_relationship(
        self,
        params: _LoadRelationParams,
    ) -> tuple[sa.Select[tuple[T]], _AbstractLoad]:
        """Select the eager-loading strategy for a single relationship hop.

        Dispatches to specialised helpers based on relationship type:

        * **Self-referential** → ``_load_self``
        * **O2M / M2M with limit** →
            ``_load_lateral_zip_m2m`` / ``_load_lateral_zip_o2m`` / ``_load_lateral``
        * **O2M / M2M without limit** → plain ``subqueryload`` / ``selectinload``
        * **M2O** → ``_load_m2o``

        Returns:
            ``(query, load)`` — the possibly-modified SELECT and the loader option.
        """
        relationship = params.relationship
        relation_cls = relationship.mapper.class_
        load = params.load
        is_alias = params.is_alias
        conditions = self.conditions or {}

        # Self-referential
        if relation_cls is self.model and relationship.parent.class_ is relation_cls:
            if not self.self_key:
                raise ValueError("`self_key` should be set for self join")

            return self._load_self(
                _LoadSelfParams(relationship=relationship, load=load),
                side="many" if relationship.uselist else "one",
            )

        # O2M / M2M
        if relationship.uselist:
            if self.limit is None:
                if not (_strategy := _MANY_LOAD_STRATEGIES.get(self.many_load)):
                    warnings.warn(
                        f"Unknown many_load strategy: {self.many_load}. Using subqueryload.",
                        stacklevel=2,
                    )
                    _strategy = orm.subqueryload

                load = _construct_strategy(_strategy, relationship, load)

                return self._query, load

            query = self._query
            adapter = self._get_clause_adapter(relationship.parent.local_table)
            rn_series = params.rn_series

            if (
                rn_series is not None
                and relationship.secondary is not None
                and self._lateral_map.get(relationship.secondary) is None
            ):
                return self._load_lateral_zip_m2m(
                    relationship,
                    relation_cls,
                    load,
                    is_alias=is_alias,
                    conditions=conditions,
                    adapter=adapter,
                    query=query,
                    rn_series=rn_series,
                )
            if rn_series is not None and relationship.secondary is None:
                return self._load_lateral_zip_o2m(
                    relationship,
                    relation_cls,
                    load,
                    is_alias=is_alias,
                    conditions=conditions,
                    adapter=adapter,
                    query=query,
                    rn_series=rn_series,
                )
            return self._load_lateral(
                relationship,
                relation_cls,
                load,
                is_alias=is_alias,
                conditions=conditions,
                adapter=adapter,
                query=query,
            )

        # M2O
        return self._load_m2o(
            relationship,
            relation_cls,
            load,
            is_alias=is_alias,
            conditions=conditions,
        )

    def _load_lateral_zip_m2m(
        self,
        relationship: orm.RelationshipProperty[orm.DeclarativeBase],
        relation_cls: type[orm.DeclarativeBase],
        load: _AbstractLoad | None,
        *,
        is_alias: bool,
        conditions: Mapping[str, Any],
        adapter: ClauseAdapter | None,
        query: sa.Select[tuple[T]],
        rn_series: sa.Subquery,
    ) -> tuple[sa.Select[tuple[T]], _AbstractLoad]:
        """ZIP M2M: self-contained LATERAL with secondary join inside the subquery."""
        secondary_table = relationship.secondary

        assert secondary_table is not None
        inner = (
            sa.select(relation_cls)
            .select_from(
                sa.join(
                    secondary_table,
                    relation_cls.__table__,
                    relationship.secondaryjoin,
                )
            )
            .where(
                adapter.traverse(relationship.primaryjoin) if adapter else relationship.primaryjoin
            )
        )
        inner = _apply_conditions(
            _apply_order_by(inner, relation_cls, self.order_by),
            relationship.key,
            conditions,  # type: ignore[arg-type]
        )
        inner = inner.limit(self.limit)
        relation_table = sa.inspect(relation_cls).local_table
        inner_sq = inner.correlate_except(relation_table, secondary_table).subquery()
        rn_col = sa.func.row_number().over().label("_sqla_rn")
        subq = sa.select(*inner_sq.c, rn_col)

        lateral_name = (
            f"{get_table_name(relation_cls)}_{relationship.key}"
            if is_alias
            else get_table_name(relation_cls)
        )
        lateral = subq.lateral(name=lateral_name)
        query = query.outerjoin(lateral, lateral.c._sqla_rn == rn_series.c._rn)  # noqa: SLF001
        load = _construct_strategy(orm.contains_eager, relationship, load, alias=lateral)
        self._lateral_map[relation_table] = lateral
        self._query = query

        return self._query, load

    def _load_lateral_zip_o2m(
        self,
        relationship: orm.RelationshipProperty[orm.DeclarativeBase],
        relation_cls: type[orm.DeclarativeBase],
        load: _AbstractLoad | None,
        *,
        is_alias: bool,
        conditions: Mapping[str, Any],
        adapter: ClauseAdapter | None,
        query: sa.Select[tuple[T]],
        rn_series: sa.Subquery,
    ) -> tuple[sa.Select[tuple[T]], _AbstractLoad]:
        inner = _apply_conditions(
            _apply_order_by(
                sa.select(relation_cls).limit(self.limit),
                relation_cls,
                self.order_by,
            ),
            relationship.key,
            conditions,  # type: ignore[arg-type]
        )
        inner = inner.where(
            adapter.traverse(relationship.primaryjoin) if adapter else relationship.primaryjoin
        )
        relation_table = sa.inspect(relation_cls).local_table
        inner_sq = inner.correlate_except(relation_table).subquery()
        rn_col = sa.func.row_number().over().label("_sqla_rn")
        subq = sa.select(*inner_sq.c, rn_col)

        lateral_name = (
            f"{get_table_name(relation_cls)}_{relationship.key}"
            if is_alias
            else get_table_name(relation_cls)
        )
        if self.check_tables and lateral_name in get_table_names(query):
            lateral_name = f"{lateral_name}_alias"
            subq = subq.correlate_except(relation_table)

        lateral = subq.lateral(name=lateral_name)
        query = query.outerjoin(lateral, lateral.c._sqla_rn == rn_series.c._rn)  # noqa: SLF001
        load = _construct_strategy(orm.contains_eager, relationship, load, alias=lateral)
        relation_table = sa.inspect(relation_cls).local_table
        self._lateral_map[relation_table] = lateral
        self._query = query

        return self._query, load

    def _load_lateral(
        self,
        relationship: orm.RelationshipProperty[orm.DeclarativeBase],
        relation_cls: type[orm.DeclarativeBase],
        load: _AbstractLoad | None,
        *,
        is_alias: bool,
        conditions: Mapping[str, Any],
        adapter: ClauseAdapter | None,
        query: sa.Select[tuple[T]],
    ) -> tuple[sa.Select[tuple[T]], _AbstractLoad]:
        """Non-ZIP LATERAL: original ON TRUE path for M2M (with reuse) and O2M."""
        subq = _apply_conditions(
            _apply_order_by(
                sa.select(relation_cls).limit(self.limit),
                relation_cls,
                self.order_by,
            ),
            relationship.key,
            conditions,  # type: ignore[arg-type]
        )
        if relationship.secondary is not None and relationship.secondaryjoin is not None:
            secondary_table = relationship.secondary
            secondary_lateral = self._lateral_map.get(secondary_table)

            if secondary_lateral is not None:
                sec_adapter = ClauseAdapter(
                    secondary_lateral,
                    equivalents={col: {secondary_lateral.c[col.key]} for col in secondary_table.c},
                )
                subq = subq.where(sec_adapter.traverse(relationship.secondaryjoin))
            else:
                subq = subq.where(relationship.secondaryjoin)
                primaryjoin = (
                    adapter.traverse(relationship.primaryjoin)
                    if adapter
                    else relationship.primaryjoin
                )
                if self.check_tables:
                    if secondary_table.description not in get_table_names(query):
                        query = query.outerjoin(secondary_table, primaryjoin)
                else:
                    query = query.outerjoin(secondary_table, primaryjoin)
        else:
            subq = subq.where(
                adapter.traverse(relationship.primaryjoin) if adapter else relationship.primaryjoin
            )

        lateral_name = (
            f"{get_table_name(relation_cls)}_{relationship.key}"
            if is_alias
            else get_table_name(relation_cls)
        )
        if self.check_tables and lateral_name in get_table_names(query):
            lateral_name = f"{lateral_name}_alias"
            relation_table = sa.inspect(relation_cls).local_table
            subq = subq.correlate_except(relation_table)

        lateral = subq.lateral(name=lateral_name)
        query = query.outerjoin(lateral, sa.true())
        load = _construct_strategy(orm.contains_eager, relationship, load, alias=lateral)
        relation_table = sa.inspect(relation_cls).local_table
        self._lateral_map[relation_table] = lateral
        self._query = query

        return self._query, load

    def _load_m2o(
        self,
        relationship: orm.RelationshipProperty[orm.DeclarativeBase],
        relation_cls: type[orm.DeclarativeBase],
        load: _AbstractLoad | None,
        *,
        is_alias: bool,
        conditions: Mapping[str, Any],
    ) -> tuple[sa.Select[tuple[T]], _AbstractLoad]:
        """M2O: selectinload (alias) or outerjoin + contains_eager / joinedload."""
        if is_alias:
            load = _construct_strategy(orm.selectinload, relationship, load)
        else:
            parent_table = get_table_name(relationship.parent.class_)
            if parent_table in get_table_names(self._query):
                self._query = _apply_conditions(
                    self._query.outerjoin(relation_cls, relationship.primaryjoin),
                    relationship.key,
                    conditions,
                )
                load = _construct_strategy(orm.contains_eager, relationship, load)
            else:
                load = _construct_strategy(orm.joinedload, relationship, load)
        return self._query, load

    def _check_zip_needs(  # noqa: C901
        self, resolved: list[Sequence[orm.RelationshipProperty[orm.DeclarativeBase]]]
    ) -> None:
        if self.limit is None:
            return

        if not self.optimization:
            return

        depth_paths: dict[int, set[str]] = {}
        for rels in resolved:
            cp = ""
            for depth, rel in enumerate(rels):
                cp = f"{cp}.{rel.key}" if cp else rel.key
                is_self_ref = (
                    rel.mapper.class_ is self.model and rel.parent.class_ is rel.mapper.class_
                )
                if rel.uselist and not is_self_ref:
                    depth_paths.setdefault(depth, set()).add(cp)

        zip_depths = sorted(d for d, paths in depth_paths.items() if len(paths) >= 2)  # noqa: PLR2004

        if not zip_depths:
            return

        max_limit = self.limit
        if self.conditions:
            zip_depth_set = set(zip_depths)
            for rels in resolved:
                cp = ""
                for depth, rel in enumerate(rels):
                    cp = f"{cp}.{rel.key}" if cp else rel.key
                    if depth in zip_depth_set and rel.uselist:
                        probe = _apply_conditions(
                            sa.select(rel.mapper.class_).limit(self.limit),
                            rel.key,
                            self.conditions,  # type: ignore[arg-type]
                        )
                        max_limit = max(max_limit, _extract_limit(probe, self.limit))

        _base = sa.select(sa.literal(1).label("_rn"))
        _cte = _base.cte(name="_sqla_rn_cte", recursive=True)
        _cte = _cte.union_all(
            sa.select((_cte.c._rn + 1).label("_rn")).where(_cte.c._rn < max_limit)  # noqa: SLF001
        )

        for idx, depth in enumerate(zip_depths):
            name = "_sqla_rn" if idx == 0 else f"_sqla_rn_{idx}"
            subq = sa.select(_cte.c._rn).subquery(name=name)  # noqa: SLF001
            self._zip_levels[depth] = subq
            self._query = self._query.outerjoin(subq, sa.true())

    def _construct(
        self, resolved: list[Sequence[orm.RelationshipProperty[orm.DeclarativeBase]]]
    ) -> None:
        # When a direct O2M targets the same table that an M2M uses as its
        # secondary, move the O2M before the M2M so its LATERAL is available
        # for reuse (avoids a duplicate raw join for the association table).
        idx: dict[sa.FromClause, list[int | None]] = {}
        for i, rels in enumerate(resolved):
            rel = rels[0]
            if (sec := rel.secondary) is not None:
                entry = idx.setdefault(sec, [None, None])
                entry[1] = i  # m2m
            else:
                target = rel.mapper.local_table
                entry = idx.setdefault(target, [None, None])
                entry[0] = i  # o2m

        for o2m_idx, m2m_idx in idx.values():
            if o2m_idx is not None and m2m_idx is not None and o2m_idx > m2m_idx:
                resolved[m2m_idx], resolved[o2m_idx] = resolved[o2m_idx], resolved[m2m_idx]

        # Auto-detect sibling LATERALs for ZIP optimization.
        # When 2+ top-level uselist relationships use LATERAL (i.e. limit is set),
        # align them via recursive CTE + ROW_NUMBER to avoid cross-product.
        self._check_zip_needs(resolved)

        for relationships in resolved:
            load = self._construct_loads(relationships)
            if load:
                self._options += load

    def _get_clause_adapter(self, table: sa.Table | sa.FromClause) -> ClauseAdapter | None:
        """Return a ClauseAdapter that remaps *table* to its lateral alias, or None."""
        lateral = self._lateral_map.get(table)
        if lateral is None:
            return None

        return ClauseAdapter(lateral, equivalents={col: {lateral.c[col.key]} for col in table.c})

    def _load_self(
        self,
        params: _LoadSelfParams,
        *,
        side: Literal["many", "one"],
    ) -> tuple[sa.Select[tuple[T]], _AbstractLoad]:
        """Handle self-referential relationship loading (e.g. Category.parent / .children).

        Uses an aliased version of the model to avoid ambiguous column references.
        ``WHERE`` clauses are rewritten via ``ClauseAdapter`` to point column references
        at the alias instead of the original table.

        When the same model has already been self-ref loaded (e.g. loading both
        ``parent`` and ``children`` on ``Category``), falls back to ``selectinload``
        to avoid identity-map conflicts. ``subqueryload`` is not used here because
        it re-embeds the original query's outerjoin, which resets already-populated
        ``noload`` attributes.

        Args:
            params: Relationship and current load chain.
            side: ``"many"`` for O2M (children), ``"one"`` for M2O (parent).

        Returns:
            ``(query, load)`` — the possibly-modified SELECT and the loader option.
        """
        load = params.load
        relationship = params.relationship
        relation_cls = relationship.mapper.class_

        # Second self-ref load on same model → selectinload to avoid identity-map conflict.
        # Must use selectinload (not subqueryload): subqueryload re-embeds the original
        # query (with its outerjoin) and resets already-populated attributes.
        # Apply conditions via .and_() on the relationship descriptor so the
        # selectinload's separate SELECT includes the WHERE clause.
        if self.model in self._self_ref_loaded:
            conditions = self.conditions or {}
            if (condition := conditions.get(relationship.key)) is not None and (
                clause := condition(sa.select(relation_cls)).whereclause
            ) is not None:
                rel_attr = getattr(relation_cls, relationship.key)
                if load is None:
                    load = orm.selectinload(rel_attr.and_(clause))
                else:
                    load = load.selectinload(rel_attr.and_(clause))

                return self._query, load

            load = _construct_strategy(orm.selectinload, relationship, load)

            return self._query, load

        conditions = self.conditions or {}

        name = f"{get_table_name(relation_cls)}_{relationship.key}"
        alias: LateralFromClause | type[orm.DeclarativeBase]

        alias = orm.aliased(relation_cls, name=name)
        original_table = sa.inspect(relation_cls).local_table
        alias_sel = sa.inspect(alias).selectable
        adapter = ClauseAdapter(
            alias_sel, equivalents={col: {alias_sel.c[col.key]} for col in original_table.c}
        )

        cond_fn = conditions.get(relationship.key) if conditions else None

        if cond_fn is not None:

            def adapted(
                q: sa.Select[tuple[T]],
                *,
                _fn: Callable[[sa.Select[tuple[T]]], sa.Select[tuple[T]]] | None = cond_fn,
                _adapter: ClauseAdapter = adapter,
                _cls: type[orm.DeclarativeBase] = relation_cls,
            ) -> sa.Select[tuple[T]]:
                assert _fn
                if (clause := _fn(sa.select(_cls)).whereclause) is not None:
                    return q.where(_adapter.traverse(clause))

                return q

            conditions = {**conditions, relationship.key: adapted}

        if side == "many":
            if self.limit:
                subq = _apply_conditions(
                    _apply_order_by(
                        sa.select(alias).limit(self.limit),
                        relation_cls,
                        self.order_by,
                    ),
                    relationship.key,
                    conditions,  # type: ignore[arg-type]
                ).where(get_primary_key(self.model) == getattr(alias, self.self_key))

                lateral = subq.lateral(name=name)
                self._query = self._query.outerjoin(lateral, sa.true())
                alias = lateral
            else:
                self._query = _apply_conditions(
                    self._query.outerjoin(
                        alias,
                        get_primary_key(self.model) == getattr(alias, self.self_key),
                    ),
                    relationship.key,
                    conditions,
                )
        else:
            pk_name = get_primary_key(self.model).key
            assert pk_name
            join_cond = getattr(self.model, self.self_key) == getattr(alias, pk_name)

            if cond_fn is not None and (clause := cond_fn(sa.select(relation_cls)).whereclause):
                join_cond = sa.and_(join_cond, adapter.traverse(clause))

            self._query = self._query.outerjoin(alias, join_cond)

        load = _construct_strategy(orm.contains_eager, relationship, load, alias=alias)

        self._self_ref_loaded.add(self.model)

        return self._query, load


@lru_cache(maxsize=2048)
def _bfs_search(
    start: type[T],
    end: str,
    node: Node,
) -> Sequence[orm.RelationshipProperty[orm.DeclarativeBase]]:
    """Perform breadth-first search to find relationship path.

    Searches for a path of relationships from a starting model to a target
    relationship key using breadth-first traversal of the relationship graph.

    Args:
        start: Starting SQLAlchemy model class.
        end: Target relationship key to find.
        node: Node instance containing relationship mappings.

    Returns:
        Sequence of relationship properties forming the path to the target.
    """
    queue: deque[
        tuple[type[orm.DeclarativeBase], list[orm.RelationshipProperty[orm.DeclarativeBase]]]
    ] = deque([(start, [])])
    seen: set[type[orm.DeclarativeBase]] = set()

    while queue:
        current, path = queue.popleft()
        if current in seen:
            continue
        seen.add(current)

        for rel in node.get(current):
            new_path = [*path, rel]
            if rel.key == end:
                return new_path

            queue.append((rel.mapper.class_, new_path))

    return ()


@lru_cache(maxsize=1028)
def _resolve_dotted_path(
    model: type[T],
    dotted: str,
    node: Node,
) -> Sequence[orm.RelationshipProperty[orm.DeclarativeBase]]:
    """Resolve a dot-notation path like 'posts.comments.reactions' into a
    sequence of RelationshipProperty objects.

    Each segment must be a direct relationship key on the current model.
    Falls back to _bfs_search if the path has no dots.
    """
    parts = dotted.split(".")
    if len(parts) == 1:
        return _bfs_search(model, dotted, node)

    result: list[orm.RelationshipProperty[orm.DeclarativeBase]] = []
    current_cls: type[orm.DeclarativeBase] = model
    for segment in parts:
        relations = node.get(current_cls)
        rel = next((r for r in relations if r.key == segment), None)
        if rel is None:
            raise ValueError(
                f"No relationship '{segment}' on {current_cls.__name__} "
                f"(resolving '{dotted}' from {model.__name__})"
            )
        result.append(rel)
        current_cls = rel.mapper.class_

    return result


def _construct_strategy(
    strategy: Callable[..., _AbstractLoad],
    relationship: orm.RelationshipProperty[T],
    current: _AbstractLoad | None = None,
    **kw: Any,
) -> _AbstractLoad:
    """Create or chain a loader strategy option.

    If ``current`` is ``None``, creates a top-level strategy (e.g. ``orm.joinedload(rel)``).
    Otherwise chains onto the existing option (e.g. ``current.joinedload(rel)``).
    """
    _strategy: _AbstractLoad = (
        strategy(relationship, **kw)
        if current is None
        else getattr(current, strategy.__name__)(relationship, **kw)
    )

    return _strategy


def _apply_conditions(
    query: sa.Select[tuple[T]],
    key: str,
    conditions: Mapping[
        str,
        Callable[[sa.Select[tuple[T]]], sa.Select[tuple[T]]],
    ],
) -> sa.Select[tuple[T]]:
    """Apply a per-relationship condition transformer to *query*."""
    return condition(query) if conditions and (condition := conditions.get(key)) else query


def _extract_limit(query: sa.Select[Any], default: int) -> int:
    """Extract integer LIMIT from a Select, or return *default*."""
    try:
        return val if (val := query._limit) is not None else default  # noqa: SLF001
    except Exception:  # noqa: BLE001
        return default


def _apply_order_by(
    query: sa.Select[tuple[T]],
    relation_cls: type[T],
    order_by: tuple[str, ...] | None = None,
) -> sa.Select[tuple[T]]:
    """Apply ORDER BY to a sub-query, defaulting to primary key descending."""
    ob = (
        (getattr(relation_cls, by).desc() for by in order_by)
        if order_by
        else (pk.desc() for pk in relation_cls.__table__.primary_key)
    )

    return query.order_by(*ob)


@lru_cache(maxsize=1028)
def _select_with_relationships(
    params: _LoadParams[T],
) -> sa.Select[tuple[T]]:
    """Build a select query with relationship loading based on load parameters.

    This is the core function that processes load parameters and constructs
    the appropriate SQLAlchemy query with joins and loading strategies.

    Args:
        params: Load parameters containing model, relationships, and options.

    Returns:
        Configured SQLAlchemy Select statement.
    """
    builder = SelectBuilder(
        model=params.model,
        node=params.node,
        limit=params.limit,
        check_tables=params.check_tables,
        conditions=params.conditions,
        self_key=params.self_key,
        order_by=params.order_by,
        many_load=params.many_load,
        distinct=params.distinct,
        optimization=params.optimization,
    )

    return builder.build(loads=params.loads, query=params.query)


@lru_cache(maxsize=128)
def _find_self_key(
    model: type[T],
) -> str:
    """Find the foreign key column for self-referential relationships.

    Looks for a foreign key in the model that references the model's own
    primary key, which indicates a self-referential relationship.

    Args:
        model: SQLAlchemy model class to examine.

    Returns:
        Name of the self-referential foreign key column, or empty string if none found.
    """
    return next(
        (
            fk.parent.name
            for fk in model.__table__.foreign_keys
            if get_primary_key(model).name == fk.column.name
            and fk.column.table.name == fk.parent.table.name
        ),
        "",
    )


def sqla_select(
    **params: Unpack[_LoadParamsType[T]],
) -> sa.Select[tuple[T]]:
    """Create a SQLAlchemy select statement with automatic relationship loading.

    This is the main function for building select queries with eager loading of relationships.
    It automatically constructs the necessary joins and load strategies based on the specified
    relationship paths.

    Args:
        model: type[T]
            The SQLAlchemy model class to select from.
        loads: tuple[str, ...]
            Tuple of relationship.key paths to load.
            Defaults to () (no relationships).
        node: Node
            Node instance containing relationship mappings.
            Defaults to None (uses singleton).
        check_tables: bool
            Whether to check for existing tables in query to avoid duplicate joins.
            Defaults to False.
        conditions: Mapping[str, Callable[[sa.Select[tuple[T]]], sa.Select[tuple[T]]]] | None
            Mapping of relationship keys to condition functions for filtering.
            Defaults to None.
        self_key: str
            Foreign key column name for self-referential relationships.
            Defaults to None (auto-detected).
        order_by: tuple[str, ...]
            Tuple of column names for ordering relationship results.
            Defaults to None (uses primary key).
        query: sa.Select[tuple[T]]
            Existing select query to extend.
            Defaults to None (creates new query).
        distinct: bool
            Whether to apply DISTINCT to the query. For edge cases, use .unique() instead.
            Defaults to False.
        limit: int | None
            Maximum number of related records to load per relationship. Defaults to 50.
        optimization: bool
            Enable ZIP optimization (ROW_NUMBER alignment for sibling LATERALs).
            Defaults to True. Set to False to use plain LATERAL ON TRUE joins.

    Returns:
        A SQLAlchemy Select statement with configured eager loading.

    Examples:
        Basic usage with conditions::

            query = sqla_select(
                model=User,
                loads=("roles", "profile"),
                conditions={
                    "roles": add_conditions(Role.active == True),
                },
                limit=10,
            )

        Deep dotted-path loading::

            query = sqla_select(
                model=User,
                loads=("posts.comments.reactions",),
            )

        Self-referential with explicit self_key::

            query = sqla_select(
                model=Category,
                loads=("children", "parent"),
                self_key="parent_id",
            )

        M2M relationship::

            query = sqla_select(model=User, loads=("roles",))

        Extending an existing query::

            base = sa.select(User).where(User.active == True)
            query = sqla_select(model=User, loads=("posts",), query=base)

        No LATERAL limit (load all related rows)::

            query = sqla_select(model=User, loads=("posts",), limit=None)
    """

    params["conditions"] = frozendict(params.get("conditions", {}))
    if "self_key" not in params:
        params["self_key"] = _find_self_key(params["model"])
    if "limit" not in params:
        params["limit"] = DEFAULT_RELATIONSHIP_LOAD_LIMIT

    return _select_with_relationships(_LoadParams[T](**params))


def sqla_cache_info() -> dict[str, Any]:
    """Return LRU cache statistics for all internal caches."""
    from .tools import _get_primary_key, _get_table_name

    return {
        fn.__name__: fn.cache_info()
        for fn in (
            _bfs_search,
            _resolve_dotted_path,
            _select_with_relationships,
            _find_self_key,
            _get_primary_key,
            _get_table_name,
        )
    }


def sqla_cache_clear() -> None:
    """Clear all internal LRU caches."""
    from .tools import _get_primary_key, _get_table_name

    for fn in (
        _bfs_search,
        _resolve_dotted_path,
        _select_with_relationships,
        _find_self_key,
        _get_primary_key,
        _get_table_name,
    ):
        fn.cache_clear()
