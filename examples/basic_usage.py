"""Basic sqla-autoloads usage examples.

Demonstrates initialization, simple loads, dotted paths,
conditions, and limit/order_by configuration.

NOTE: This file is illustrative — it won't run standalone
without a database and seeded data.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from sqla_autoloads import add_conditions, get_node, init_node, sqla_select

from .models import Base, Category, Post, Role, User


# ── 1. Initialize once at startup ────────────────────────────────────

engine = create_async_engine("sqlite+aiosqlite:///:memory:")


async def setup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Call once — builds a singleton graph of all relationships
    init_node(get_node(Base))


# ── 2. Simple loads ──────────────────────────────────────────────────


async def get_users_with_posts(session: AsyncSession) -> list[User]:
    query = sqla_select(model=User, loads=("posts",))
    result = await session.execute(query)
    return list(result.unique().scalars().all())


async def get_users_with_all(session: AsyncSession) -> list[User]:
    query = sqla_select(model=User, loads=("posts", "roles", "profile"))
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# ── 3. Dotted / deep paths ──────────────────────────────────────────


async def get_users_deep(session: AsyncSession) -> list[User]:
    query = sqla_select(model=User, loads=("posts.comments.reactions",))
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# ── 4. Conditions ────────────────────────────────────────────────────


async def get_users_with_senior_roles(session: AsyncSession) -> list[User]:
    query = sqla_select(
        model=User,
        loads=("roles",),
        conditions={
            "roles": add_conditions(Role.level > 3),  # noqa: PLR2004
        },
    )
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# ── 5. Limit and order_by ────────────────────────────────────────────


async def get_users_latest_5_posts(session: AsyncSession) -> list[User]:
    query = sqla_select(model=User, loads=("posts",), limit=5)
    result = await session.execute(query)
    return list(result.unique().scalars().all())


async def get_users_posts_by_title(session: AsyncSession) -> list[User]:
    query = sqla_select(
        model=User,
        loads=("posts",),
        order_by=("title",),
    )
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# ── 6. No limit (subqueryload / selectinload) ───────────────────────


async def get_users_all_posts(session: AsyncSession) -> list[User]:
    query = sqla_select(model=User, loads=("posts",), limit=None)
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# ── 7. Extending an existing query ──────────────────────────────────


async def get_active_users_with_posts(session: AsyncSession) -> list[User]:
    base = sa.select(User).where(User.name != "deleted")
    query = sqla_select(model=User, loads=("posts",), query=base)
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# ── 8. Self-referential ─────────────────────────────────────────────


async def get_categories(session: AsyncSession) -> list[Category]:
    query = sqla_select(
        model=Category,
        loads=("children", "parent"),
        self_key="parent_id",
    )
    result = await session.execute(query)
    return list(result.unique().scalars().all())


# ── 9. M2O (many-to-one) ────────────────────────────────────────────


async def get_posts_with_author(session: AsyncSession) -> list[Post]:
    query = sqla_select(model=Post, loads=("author",))
    result = await session.execute(query)
    return list(result.unique().scalars().all())
