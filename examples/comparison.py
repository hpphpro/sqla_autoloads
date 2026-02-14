"""Before/after comparison: raw SQLAlchemy vs sqla-autoloads.

Shows how the same query looks with manual join construction
versus a single sqla_select call.
"""

from __future__ import annotations

from typing import Any, Literal

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from .models import Post, Profile, Role, User, user_roles


UserLoad = Literal["posts", "roles", "profile"]


# usually its not the case but anyway you should build your wants separately or like this


async def get_users_raw(
    session: AsyncSession,
    *loads: UserLoad,
) -> list[User]:
    query = sa.select(User)
    options: list[Any] = []

    if "posts" in loads:
        lateral = (
            sa.select(Post)
            .where(Post.author_id == User.id)
            .order_by(Post.id.desc())
            .limit(50)
            .lateral()
        )
        query = query.outerjoin(lateral, sa.true())
        options.append(orm.contains_eager(User.posts, alias=lateral))

    if "roles" in loads:
        query = query.outerjoin(user_roles, User.id == user_roles.c.user_id).outerjoin(
            Role, user_roles.c.role_id == Role.id
        )
        options.append(orm.contains_eager(User.roles))

    if "profile" in loads:
        query = query.outerjoin(Profile, Profile.user_id == User.id)
        options.append(orm.contains_eager(User.profile))

    if options:
        query = query.options(*options)

    result = await session.execute(query)
    return list(result.unique().scalars().all())


async def get_users_autoloads(
    session: AsyncSession,
    *loads: UserLoad,
) -> list[User]:
    query = sqla_select(model=User, loads=loads)
    result = await session.execute(query)
    return list(result.unique().scalars().all())
