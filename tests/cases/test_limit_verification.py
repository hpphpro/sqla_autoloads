"""Verify that LATERAL limit actually caps the number of related records loaded.

Uses a seed_20 fixture with 20 posts for alice and 20 comments on post 1,
so limits below 20 can be verified.
"""
from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Comment, Post, User

pytestmark = [pytest.mark.anyio, pytest.mark.lateral]

@pytest.fixture
async def seed_20(session: AsyncSession) -> None:

    alice = User(id=1, name="alice", active=True)
    session.add(alice)
    await session.flush()

    posts = [Post(id=i, title=f"Post {i}", body=f"body{i}", author_id=1) for i in range(1, 21)]
    session.add_all(posts)
    await session.flush()

    comments = [Comment(id=i, text=f"Comment {i}", post_id=1) for i in range(1, 21)]
    session.add_all(comments)
    await session.flush()

    session.expunge_all()


class TestLimitVerification:
    @pytest.mark.parametrize("limit", [5, 10, 15], ids=["limit_5", "limit_10", "limit_15"])
    async def test_o2m_limit_respected(self, session: AsyncSession, seed_20: None, limit: int) -> None:
        """sqla_select with limit=N returns exactly N posts (out of 20)."""
        query = sqla_select(model=User, loads=("posts",), limit=limit)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == limit

    async def test_no_limit_returns_all(self, session: AsyncSession, seed_20: None) -> None:
        """limit=None returns all 20."""
        query = sqla_select(model=User, loads=("posts",), limit=None)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 20

    async def test_manual_sqla_has_no_limit(self, session: AsyncSession, seed_20: None) -> None:
        """Raw SQLAlchemy subqueryload returns all 20 -- no per-parent limit."""
        query = sa.select(User).options(orm.subqueryload(User.posts))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 20

    async def test_deep_chain_limit(self, session: AsyncSession, seed_20: None) -> None:
        """Limit applies at each level: 5 posts, each with up to 5 comments."""
        query = sqla_select(model=User, loads=("posts.comments",), limit=5)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 5

        # Only post 1 has comments; limit=5 returns up to 5
        post1 = next((p for p in alice.posts if p.id == 1), None)
        if post1:
            assert len(post1.comments) == 5
