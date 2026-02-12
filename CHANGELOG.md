# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
