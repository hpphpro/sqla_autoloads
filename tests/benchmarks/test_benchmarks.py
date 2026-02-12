"""Async execution benchmarks: sqla_select vs manual SQLAlchemy.

Measures actual query execution time (not just query building).
Run with: uv run pytest tests/benchmarks/ -v -s
Skip with: uv run pytest tests/ -m "not benchmark"
"""
from __future__ import annotations

import time

import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Comment, Post, User
from typing import Callable, Any, Final

pytestmark = [pytest.mark.anyio, pytest.mark.benchmark, pytest.mark.lateral]


N: Final[int] = 100


async def _measure(session: AsyncSession, fn: Callable[[], sa.Select[Any]], n: int = N) -> float:
    # Execute fn() n times, return total seconds.

    # Warm up
    r = await session.execute(fn())
    r.unique().scalars().all()
    session.expunge_all()

    start = time.perf_counter()
    for _ in range(n):
        result = await session.execute(fn())
        result.unique().scalars().all()
        session.expunge_all()

    return time.perf_counter() - start


def _fmt(label: str, elapsed: float, n: int = N) -> str:
    return f"    {label:<30s} {elapsed:.3f}s ({n} queries, {elapsed / n * 1000:.1f}ms/q)"


class TestExecutionBenchmarks:
    async def test_o2m_posts(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # O2M: User.posts — lateral vs no_limit vs manual subqueryload.
        t_lat = await _measure(session, lambda: sqla_select(model=User, loads=("posts",)))
        t_nol = await _measure(
            session, lambda: sqla_select(model=User, loads=("posts",), limit=None)
        )
        t_man = await _measure(
            session, lambda: sa.select(User).options(orm.subqueryload(User.posts))
        )

        print(f"\n  O2M posts ({N} queries):")
        print(_fmt("sqla_select (lateral):", t_lat))
        print(_fmt("sqla_select (no_limit):", t_nol))
        print(_fmt("manual SQLAlchemy:", t_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")

    async def test_m2o_author(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # M2O: Post.author — sqla_select vs manual joinedload.
        t_lat = await _measure(session, lambda: sqla_select(model=Post, loads=("author",)))
        t_nol = await _measure(
            session, lambda: sqla_select(model=Post, loads=("author",), limit=None)
        )
        t_man = await _measure(
            session, lambda: sa.select(Post).options(orm.joinedload(Post.author))
        )

        print(f"\n  M2O author ({N} queries):")
        print(_fmt("sqla_select (lateral):", t_lat))
        print(_fmt("sqla_select (no_limit):", t_nol))
        print(_fmt("manual SQLAlchemy:", t_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")

    async def test_m2m_roles(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # M2M: User.roles — lateral vs no_limit vs manual selectinload.
        t_lat = await _measure(session, lambda: sqla_select(model=User, loads=("roles",)))
        t_nol = await _measure(
            session, lambda: sqla_select(model=User, loads=("roles",), limit=None)
        )
        t_man = await _measure(
            session, lambda: sa.select(User).options(orm.selectinload(User.roles))
        )

        print(f"\n  M2M roles ({N} queries):")
        print(_fmt("sqla_select (lateral):", t_lat))
        print(_fmt("sqla_select (no_limit):", t_nol))
        print(_fmt("manual SQLAlchemy:", t_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")

    async def test_all_direct(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # All 6 direct User relationships.
        all_loads = ("posts", "roles", "sent_messages", "received_messages", "owned_messages", "profile")
        t_lat = await _measure(session, lambda: sqla_select(model=User, loads=all_loads))
        t_nol = await _measure(
            session, lambda: sqla_select(model=User, loads=all_loads, limit=None)
        )
        t_man = await _measure(
            session,
            lambda: sa.select(User).options(
                orm.subqueryload(User.posts),
                orm.selectinload(User.roles),
                orm.subqueryload(User.sent_messages),
                orm.subqueryload(User.received_messages),
                orm.subqueryload(User.owned_messages),
                orm.joinedload(User.profile),
            ),
        )

        print(f"\n  All direct ({N} queries):")
        print(_fmt("sqla_select (lateral):", t_lat))
        print(_fmt("sqla_select (no_limit):", t_nol))
        print(_fmt("manual SQLAlchemy:", t_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")

    async def test_deep_chain(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # Deep chain: User -> posts -> comments -> reactions.
        t_lat = await _measure(
            session, lambda: sqla_select(model=User, loads=("posts.comments.reactions",))
        )
        t_nol = await _measure(
            session,
            lambda: sqla_select(model=User, loads=("posts.comments.reactions",), limit=None),
        )
        t_man = await _measure(
            session,
            lambda: sa.select(User).options(
                orm.subqueryload(User.posts)
                .subqueryload(Post.comments)
                .subqueryload(Comment.reactions)
            ),
        )

        print(f"\n  Deep chain ({N} queries):")
        print(_fmt("sqla_select (lateral):", t_lat))
        print(_fmt("sqla_select (no_limit):", t_nol))
        print(_fmt("manual SQLAlchemy:", t_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")

    async def test_all_plus_deep(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # All direct + deep chains.
        all_loads = (
            "posts", "roles", "sent_messages", "received_messages",
            "owned_messages", "profile",
            "posts.comments.reactions", "posts.tags", "posts.attachments",
        )
        t_lat = await _measure(session, lambda: sqla_select(model=User, loads=all_loads))
        t_nol = await _measure(
            session, lambda: sqla_select(model=User, loads=all_loads, limit=None)
        )
        t_man = await _measure(
            session,
            lambda: sa.select(User).options(
                orm.subqueryload(User.posts)
                .subqueryload(Post.comments)
                .subqueryload(Comment.reactions),
                orm.subqueryload(User.posts).selectinload(Post.tags),
                orm.subqueryload(User.posts).subqueryload(Post.attachments),
                orm.selectinload(User.roles),
                orm.subqueryload(User.sent_messages),
                orm.subqueryload(User.received_messages),
                orm.subqueryload(User.owned_messages),
                orm.joinedload(User.profile),
            ),
        )

        print(f"\n  All + deep ({N} queries):")
        print(_fmt("sqla_select (lateral):", t_lat))
        print(_fmt("sqla_select (no_limit):", t_nol))
        print(_fmt("manual SQLAlchemy:", t_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")
