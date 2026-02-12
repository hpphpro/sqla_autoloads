# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-02-12

### Fixed
- Self-ref conditions on second relationship: when loading both `parent` and `children` on the same model, conditions on the second (selectinload fallback) were silently dropped. Now applied via `.and_()` on the relationship descriptor.
- LATERAL `check_tables` collision with pre-joined table: when the base query already contains the relation table (e.g. via outerjoin), the `_alias` suffix was applied but auto-correlation broke the subquery. Now uses `correlate_except()` to preserve the relation table's FROM clause.

### Added
- Tests for self-ref both-side conditions (`TestSelfRefBothConditions`)
- Tests for LATERAL alias naming with `check_tables` (`TestCheckTablesNaming`)
- Tests for external `.where()` filtering on LATERAL alias names (`TestExternalWhereWithLateral`)
- Tests for `check_tables` with multiple foreign keys (`test_check_tables_three_fks`, `test_user_side_all_messages_check_tables`)
- Test for `_alias` suffix on LATERAL name collision with `check_tables=True`

## [0.1.0] - 2026-02-12

### Added
- Initial release
- `sqla_select` — automatic eager-loading with LATERAL subqueries, dotted paths, self-referential handling
- `init_node` / `get_node` — relationship graph initialization from declarative base
- `add_conditions` — per-relationship WHERE filtering
- `SelectBuilder` — query builder with automatic strategy selection
- Support for O2M, M2O, O2O, M2M (association table) relationships
- Multiple FK support (e.g. `from_user`, `to_user`, `owner` on same table)
- LATERAL limit with configurable per-relationship cap (default 50)
- Deep/dotted path traversal (`"posts.comments.reactions"`)
- Self-referential relationship handling (parent/children)
- LRU-cached query construction
- Async-first, sync-compatible
- Full type annotations (PEP 561 `py.typed` marker)
- Python 3.10–3.13 support
