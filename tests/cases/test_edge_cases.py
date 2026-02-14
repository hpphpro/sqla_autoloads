from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import add_conditions, sqla_select

from ..models import Attachment, Base, Post, Profile, User

pytestmark = pytest.mark.anyio


class TestEdgeCases:
    @pytest.mark.lateral
    async def test_limit_zero(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=0)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0
        for user in users:
            assert len(user.posts) == 0

    @pytest.mark.lateral
    async def test_limit_one(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",), limit=1)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) <= 1

    async def test_nonexistent_key_in_loads(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("nonexistent_rel",))
        result = await session.execute(query)
        users = result.scalars().all()

        assert len(users) == 3

    async def test_empty_loads_with_conditions(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=(),
            conditions={"posts": add_conditions(Post.title == "Alice Post 1")},
        )
        result = await session.execute(query)
        users = result.scalars().all()

        assert len(users) == 3

    async def test_query_with_where_and_loads(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        base_query = sa.select(User).where(User.name == "alice")
        query = sqla_select(model=User, loads=("posts",), query=base_query)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) == 1
        assert users[0].name == "alice"
        assert len(users[0].posts) > 0

    async def test_duplicate_loads_no_crash(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts", "posts"))
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0

    @pytest.mark.lateral
    async def test_negative_limit_raises(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Negative limit is rejected by the database."""
        query = sqla_select(model=User, loads=("posts",), limit=-1)
        with pytest.raises(sa.exc.DBAPIError):
            await session.execute(query)

    async def test_empty_m2m(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """User with no roles returns empty list for M2M."""
        query = sqla_select(model=User, loads=("roles",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        charlie = next(u for u in users if u.name == "charlie")
        assert charlie.roles == []

    async def test_o2o_with_conditions(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """O2O (Profile) with conditions."""
        query = sqla_select(
            model=User,
            loads=("profile",),
            conditions={"profile": add_conditions(Profile.bio == "Alice bio")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        assert alice.profile is not None
        assert alice.profile.bio == "Alice bio"

    async def test_o2o_null(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """User without profile gets None."""
        query = sqla_select(model=User, loads=("profile",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        charlie = next(u for u in users if u.name == "charlie")
        assert charlie.profile is None

    @pytest.mark.lateral
    async def test_m2m_limit_zero(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """M2M with limit=0 loads empty list."""
        query = sqla_select(model=User, loads=("roles",), limit=0)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        for user in users:
            assert user.roles == []

    @pytest.mark.lateral
    async def test_polymorphic_with_conditions(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Polymorphic relationship (Post.attachments) with conditions."""
        query = sqla_select(
            model=Post,
            loads=("attachments",),
            conditions={"attachments": add_conditions(Attachment.url.like("%img%"))},
        )
        result = await session.execute(query)
        posts = result.unique().scalars().all()
        post1 = next(p for p in posts if p.title == "Alice Post 1")
        assert len(post1.attachments) == 2
        for att in post1.attachments:
            assert "img" in att.url
