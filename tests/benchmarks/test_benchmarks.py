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

from sqla_autoloads import sqla_select, unique_scalars

from ..models import (
    Base,
    Comment,
    Post,
    PostTag,
    Reaction,
    Role,
    Tag,
    User,
    user_roles,
)
from typing import Callable, Any, Final

pytestmark = [pytest.mark.anyio, pytest.mark.benchmark, pytest.mark.lateral]


N: Final[int] = 100
N_LARGE: Final[int] = 20


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


async def _measure_with_counts(
    session: AsyncSession, fn: Callable[[], sa.Select[Any]], n: int = N_LARGE
) -> tuple[float, int, int]:
    """Returns (elapsed_seconds, raw_row_count, unique_entity_count)."""
    # Warm up
    r = await session.execute(fn())
    r.unique().scalars().all()
    session.expunge_all()

    start = time.perf_counter()
    for _ in range(n):
        result = await session.execute(fn())
        result.unique().scalars().all()
        session.expunge_all()
    elapsed = time.perf_counter() - start

    # Raw row count via connection-level execution (bypasses ORM unique requirement)
    query = fn()
    conn = await session.connection()
    raw_result = await conn.execute(query)
    raw_count = len(raw_result.all())

    # Unique entity count via ORM
    result = await session.execute(fn())
    unique_count = len(unique_scalars(result))
    session.expunge_all()

    return elapsed, raw_count, unique_count


def _fmt(label: str, elapsed: float, n: int = N) -> str:
    return f"    {label:<30s} {elapsed:.3f}s ({n} queries, {elapsed / n * 1000:.1f}ms/q)"


def _fmt_large(label: str, elapsed: float, raw: int, unique: int, n: int = N_LARGE) -> str:
    return (
        f"    {label:<30s} {elapsed:.3f}s ({n}q, {elapsed / n * 1000:.1f}ms/q) "
        f"raw={raw} unique={unique}"
    )


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


# ---------------------------------------------------------------------------
# Data-heavy benchmarks — substantial row counts to show LATERAL/ZIP impact
# ---------------------------------------------------------------------------

@pytest.fixture
async def seed_bench(session: AsyncSession) -> dict[str, int]:
    """Seed 10 users, 20 posts/user, 10 comments/post, 3 reactions/comment,
    5 roles (2-3/user), 5 tags (1-2/post). ~8,500 rows total."""

    users = [User(id=i, name=f"user_{i}", active=True) for i in range(1, 11)]
    session.add_all(users)
    await session.flush()

    # 20 posts per user = 200 posts
    posts = []
    post_id = 1
    for u in users:
        for j in range(1, 21):
            posts.append(Post(id=post_id, title=f"Post {j} by {u.name}", body="x", author_id=u.id))
            post_id += 1
    session.add_all(posts)
    await session.flush()

    # 10 comments per post = 2,000 comments
    comments = []
    comment_id = 1
    for p in posts:
        for j in range(1, 11):
            comments.append(Comment(id=comment_id, text=f"Comment {j}", post_id=p.id))
            comment_id += 1
    session.add_all(comments)
    await session.flush()

    # 3 reactions per comment = 6,000 reactions
    reactions = []
    reaction_id = 1
    emojis = ["\U0001f44d", "\u2764\ufe0f", "\U0001f525"]
    for c in comments:
        for emoji in emojis:
            reactions.append(Reaction(id=reaction_id, emoji=emoji, comment_id=c.id))
            reaction_id += 1
    session.add_all(reactions)
    await session.flush()

    # 5 roles
    roles = [Role(id=i, name=f"role_{i}", level=i * 2) for i in range(1, 6)]
    session.add_all(roles)
    await session.flush()

    # 2-3 roles per user
    role_rows = []
    for u in users:
        assigned = [1, 2] if u.id % 2 == 0 else [1, 2, 3]
        for rid in assigned:
            role_rows.append({"user_id": u.id, "role_id": rid})
    await session.execute(user_roles.insert().values(role_rows))
    await session.flush()

    # 5 tags
    tags = [Tag(id=i, name=f"tag_{i}") for i in range(1, 6)]
    session.add_all(tags)
    await session.flush()

    # 1-2 tags per post (~300 post_tags)
    pt_rows = []
    for p in posts:
        tag_ids = [1, 2] if p.id % 3 == 0 else [1]
        for tid in tag_ids:
            pt_rows.append(PostTag(post_id=p.id, tag_id=tid))
    session.add_all(pt_rows)
    await session.flush()

    session.expunge_all()

    return {
        "users": 10,
        "posts": len(posts),
        "comments": len(comments),
        "reactions": len(reactions),
        "roles": len(roles),
        "tags": len(tags),
    }


class TestDataBenchmarks:
    async def test_bench_o2m_large(
        self, session: AsyncSession, seed_bench: dict[str, int]
    ) -> None:
        """O2M: User.posts with 20 posts/user."""
        t_lat, raw_lat, uniq_lat = await _measure_with_counts(
            session, lambda: sqla_select(model=User, loads=("posts",), limit=5)
        )
        t_nol, raw_nol, uniq_nol = await _measure_with_counts(
            session, lambda: sqla_select(model=User, loads=("posts",), limit=None)
        )
        t_man, raw_man, uniq_man = await _measure_with_counts(
            session, lambda: sa.select(User).options(orm.subqueryload(User.posts))
        )

        print(f"\n  O2M posts LARGE ({N_LARGE} queries, {seed_bench['posts']} posts):")
        print(_fmt_large("sqla_select (lateral=5):", t_lat, raw_lat, uniq_lat))
        print(_fmt_large("sqla_select (no_limit):", t_nol, raw_nol, uniq_nol))
        print(_fmt_large("manual SQLAlchemy:", t_man, raw_man, uniq_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")

    async def test_bench_deep_chain_large(
        self, session: AsyncSession, seed_bench: dict[str, int]
    ) -> None:
        """Deep chain: posts.comments (20 posts x 10 comments)."""
        t_lat, raw_lat, uniq_lat = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=("posts.comments",), limit=5),
        )
        t_nol, raw_nol, uniq_nol = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=("posts.comments",), limit=None),
        )
        t_man, raw_man, uniq_man = await _measure_with_counts(
            session,
            lambda: sa.select(User).options(
                orm.subqueryload(User.posts).subqueryload(Post.comments)
            ),
        )

        print(f"\n  Deep chain LARGE ({N_LARGE} queries, {seed_bench['comments']} comments):")
        print(_fmt_large("sqla_select (lateral=5):", t_lat, raw_lat, uniq_lat))
        print(_fmt_large("sqla_select (no_limit):", t_nol, raw_nol, uniq_nol))
        print(_fmt_large("manual SQLAlchemy:", t_man, raw_man, uniq_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")

    async def test_bench_wide_siblings_large(
        self, session: AsyncSession, seed_bench: dict[str, int]
    ) -> None:
        """Wide siblings: (posts, roles) with ZIP."""
        t_lat, raw_lat, uniq_lat = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=("posts", "roles"), limit=5),
        )
        t_nol, raw_nol, uniq_nol = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=("posts", "roles"), limit=None),
        )
        t_man, raw_man, uniq_man = await _measure_with_counts(
            session,
            lambda: sa.select(User).options(
                orm.subqueryload(User.posts),
                orm.selectinload(User.roles),
            ),
        )

        t_nozip, raw_nozip, uniq_nozip = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=("posts", "roles"), limit=5, optimization=False),
        )

        print(f"\n  Wide siblings LARGE ({N_LARGE} queries, ZIP posts+roles):")
        print(_fmt_large("sqla_select (lateral=5):", t_lat, raw_lat, uniq_lat))
        print(_fmt_large("sqla_select (lat, no zip):", t_nozip, raw_nozip, uniq_nozip))
        print(_fmt_large("sqla_select (no_limit):", t_nol, raw_nol, uniq_nol))
        print(_fmt_large("manual SQLAlchemy:", t_man, raw_man, uniq_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")
        print(f"    ratio (zip/nozip):      {t_lat / t_nozip:.2f}x")

    async def test_bench_all_deep_large(
        self, session: AsyncSession, seed_bench: dict[str, int]
    ) -> None:
        """All deep: posts.comments.reactions, roles, posts.tags."""
        all_loads = ("posts.comments.reactions", "roles", "posts.tags")

        t_lat, raw_lat, uniq_lat = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=all_loads, limit=5),
        )
        t_nol, raw_nol, uniq_nol = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=all_loads, limit=None),
        )
        t_man, raw_man, uniq_man = await _measure_with_counts(
            session,
            lambda: sa.select(User).options(
                orm.subqueryload(User.posts)
                .subqueryload(Post.comments)
                .subqueryload(Comment.reactions),
                orm.subqueryload(User.posts).selectinload(Post.tags),
                orm.selectinload(User.roles),
            ),
        )

        t_nozip, raw_nozip, uniq_nozip = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=all_loads, limit=5, optimization=False),
        )

        print(f"\n  All deep LARGE ({N_LARGE} queries, full graph):")
        print(_fmt_large("sqla_select (lateral=5):", t_lat, raw_lat, uniq_lat))
        print(_fmt_large("sqla_select (lat, no zip):", t_nozip, raw_nozip, uniq_nozip))
        print(_fmt_large("sqla_select (no_limit):", t_nol, raw_nol, uniq_nol))
        print(_fmt_large("manual SQLAlchemy:", t_man, raw_man, uniq_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")
        print(f"    ratio (zip/nozip):      {t_lat / t_nozip:.2f}x")

    async def test_bench_zip_overhead_small_cardinality(
        self, session: AsyncSession, seed_bench: dict[str, int]
    ) -> None:
        """ZIP overhead on small cardinality: roles (2-3/user) + posts.tags (1-2/post).

        Few items per relationship means the CTE overhead exceeds
        cross-product elimination benefit.
        """
        loads = ("roles", "posts.tags")

        t_lat, raw_lat, uniq_lat = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=loads, limit=5),
        )
        t_nozip, raw_nozip, uniq_nozip = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=loads, limit=5, optimization=False),
        )
        t_nol, raw_nol, uniq_nol = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=loads, limit=None),
        )
        t_man, raw_man, uniq_man = await _measure_with_counts(
            session,
            lambda: sa.select(User).options(
                orm.selectinload(User.roles),
                orm.subqueryload(User.posts).selectinload(Post.tags),
            ),
        )

        print(f"\n  ZIP overhead small cardinality ({N_LARGE} queries, roles + posts.tags):")
        print(_fmt_large("sqla_select (lateral+ZIP):", t_lat, raw_lat, uniq_lat))
        print(_fmt_large("sqla_select (lat, no zip):", t_nozip, raw_nozip, uniq_nozip))
        print(_fmt_large("sqla_select (no_limit):", t_nol, raw_nol, uniq_nol))
        print(_fmt_large("manual SQLAlchemy:", t_man, raw_man, uniq_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")
        print(f"    ratio (zip/nozip):      {t_lat / t_nozip:.2f}x")

    async def test_bench_many_laterals_overhead(
        self, session: AsyncSession, seed_bench: dict[str, int]
    ) -> None:
        """Many LATERAL joins: all User relationships with limit=5.

        One complex query with 7+ LATERAL joins + CTE vs separate
        subqueryload/selectinload queries. Shows lateral overhead
        on many-relationship scenarios.
        """
        all_loads = (
            "posts", "roles", "sent_messages", "received_messages",
            "owned_messages", "posts.comments.reactions", "posts.tags",
        )

        t_lat, raw_lat, uniq_lat = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=all_loads, limit=5),
        )
        t_nozip, raw_nozip, uniq_nozip = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=all_loads, limit=5, optimization=False),
        )
        t_nol, raw_nol, uniq_nol = await _measure_with_counts(
            session,
            lambda: sqla_select(model=User, loads=all_loads, limit=None),
        )
        t_man, raw_man, uniq_man = await _measure_with_counts(
            session,
            lambda: sa.select(User).options(
                orm.subqueryload(User.posts)
                .subqueryload(Post.comments)
                .subqueryload(Comment.reactions),
                orm.subqueryload(User.posts).selectinload(Post.tags),
                orm.selectinload(User.roles),
                orm.subqueryload(User.sent_messages),
                orm.subqueryload(User.received_messages),
                orm.subqueryload(User.owned_messages),
            ),
        )

        print(f"\n  Many LATERALs overhead ({N_LARGE} queries, 7 relationships):")
        print(_fmt_large("sqla_select (lateral=5):", t_lat, raw_lat, uniq_lat))
        print(_fmt_large("sqla_select (lat, no zip):", t_nozip, raw_nozip, uniq_nozip))
        print(_fmt_large("sqla_select (no_limit):", t_nol, raw_nol, uniq_nol))
        print(_fmt_large("manual SQLAlchemy:", t_man, raw_man, uniq_man))
        print(f"    ratio (manual/lateral): {t_man / t_lat:.2f}x")
        print(f"    ratio (zip/nozip):      {t_lat / t_nozip:.2f}x")
