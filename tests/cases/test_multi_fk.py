from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, Message, User

pytestmark = pytest.mark.anyio

class TestMultiForeignKey:
    async def test_from_user_only(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Message, loads=("from_user",))
        result = await session.execute(query)
        messages = result.unique().scalars().all()
        msg1 = next(m for m in messages if m.id == 1)

        assert msg1.from_user.name == "alice"

    async def test_to_user_only(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Message, loads=("to_user",))
        result = await session.execute(query)
        messages = result.unique().scalars().all()
        msg1 = next(m for m in messages if m.id == 1)

        assert msg1.to_user.name == "bob"

    async def test_owner_only(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Message, loads=("owner",))
        result = await session.execute(query)
        messages = result.unique().scalars().all()
        msg1 = next(m for m in messages if m.id == 1)

        assert msg1.owner.name == "alice"

    async def test_two_fks_simultaneously(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Message, loads=("from_user", "to_user"))
        result = await session.execute(query)
        messages = result.unique().scalars().all()
        msg1 = next(m for m in messages if m.id == 1)

        assert msg1.from_user.name == "alice"
        assert msg1.to_user.name == "bob"

    async def test_all_three_fks(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Message, loads=("from_user", "to_user", "owner"))
        result = await session.execute(query)
        messages = result.unique().scalars().all()
        msg1 = next(m for m in messages if m.id == 1)

        assert msg1.from_user.name == "alice"
        assert msg1.to_user.name == "bob"
        assert msg1.owner.name == "alice"

    async def test_user_side_sent_messages(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("sent_messages",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.sent_messages) == 2

    async def test_user_side_received_messages(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("received_messages",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        bob = next(u for u in users if u.name == "bob")

        assert len(bob.received_messages) == 1

    async def test_user_side_owned_messages(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("owned_messages",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        charlie = next(u for u in users if u.name == "charlie")

        assert len(charlie.owned_messages) == 1

    async def test_alias_used_for_duplicate_table(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=Message, loads=("from_user", "to_user"))
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "users" in sql_text
