"""Self-referential relationship loading example.

Demonstrates loading parent/children on the same model (Category).
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from .models import Category


# self_key is not important here since it's auto-detected, it's just for the example


async def get_categories_with_children(session: AsyncSession) -> list[Category]:
    query = sqla_select(
        model=Category,
        loads=("children",),
        self_key="parent_id",
    )
    result = await session.execute(query)
    return list(result.unique().scalars().all())


async def get_categories_with_parent(session: AsyncSession) -> list[Category]:
    query = sqla_select(
        model=Category,
        loads=("parent",),
        self_key="parent_id",
    )
    result = await session.execute(query)
    return list(result.unique().scalars().all())


async def get_categories_full(session: AsyncSession) -> list[Category]:
    query = sqla_select(
        model=Category,
        loads=("children", "parent"),
        self_key="parent_id",
    )
    result = await session.execute(query)
    return list(result.unique().scalars().all())
