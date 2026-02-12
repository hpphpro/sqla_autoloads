from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import sqla_select

from ..models import Base, User


pytestmark = pytest.mark.anyio





class TestDottedBasic:
    async def test_dot_notation_two_hop(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None  :
        query = sqla_select(model=User, loads=("posts.comments",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        all_comments = []
        for post in alice.posts:
            all_comments.extend(post.comments)
        assert len(all_comments) == 2

    async def test_dot_notation_three_hop(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts.comments.reactions",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)

        assert len(all_reactions) == 2

    async def test_mixed_dot_and_simple(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts.comments.reactions", "roles"))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        role_names = {r.name for r in alice.roles}

        assert role_names == {"admin", "editor"}
        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)
        assert len(all_reactions) == 2

    async def test_dot_overlapping_prefix(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts.comments.reactions", "posts"))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)

        assert len(all_reactions) == 2

    async def test_dot_two_deep_paths(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts.comments.reactions", "sent_messages"))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        all_reactions = []
        for post in alice.posts:
            for comment in post.comments:
                all_reactions.extend(comment.reactions)

        assert len(all_reactions) == 2
        assert len(alice.sent_messages) == 2


class TestDottedCircular:
    async def test_dot_circular_m2o_back_to_root(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        """posts.author goes Post -> User, which is the root model.
        Should use selectinload (is_alias=True) since User is already in query.
        """
        query = sqla_select(model=User, loads=("posts.author",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
        for post in alice.posts:
            assert post.author is not None
            assert post.author.name == "alice"

    async def test_dot_sent_messages_to_user(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # sent_messages.to_user: Message.to_user -> User, circular back to root.
        query = sqla_select(model=User, loads=("sent_messages.to_user",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.sent_messages) == 2

        to_names = {m.to_user.name for m in alice.sent_messages}

        assert to_names == {"bob", "charlie"}

    async def test_dot_multiple_message_users(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # Two paths through Message back to User: sent_messages.to_user, sent_messages.owner.
        query = sqla_select(
            model=User, loads=("sent_messages.to_user", "sent_messages.owner")
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.sent_messages) == 2
        for msg in alice.sent_messages:
            assert msg.to_user is not None
            assert msg.owner is not None

    async def test_dot_all_three_message_users(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        # Three paths through Message back to User.
        query = sqla_select(
            model=User,
            loads=("sent_messages.from_user", "sent_messages.to_user", "sent_messages.owner"),
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.sent_messages) == 2
        for msg in alice.sent_messages:
            assert msg.from_user is not None
            assert msg.to_user is not None
            assert msg.owner is not None

    async def test_dot_deep_circular_chain(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        """4-hop: User -> Post -> Comment -> Post -> User.
        Post appears twice (alias needed on second), User is circular.
        """
        query = sqla_select(model=User, loads=("posts.comments.post.author",))
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 3
        for post in alice.posts:
            for comment in post.comments:
                assert comment.post is not None
                assert comment.post.author is not None
