from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import AsyncSession

from sqla_autoloads import add_conditions, sqla_select

from ..models import Base, Category, Message, Post, Tag, User, user_roles

pytestmark = pytest.mark.anyio


class TestQueryFeatures:
    async def test_distinct(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), distinct=True)
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

    async def test_existing_query_with_loads(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        base_query = sa.select(User).where(User.active == True)  # noqa: E712
        query = sqla_select(model=User, loads=("posts",), query=base_query)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        names = {u.name for u in users}

        assert "charlie" not in names

    async def test_query_preserves_where(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        base_query = sa.select(User).where(User.name == "alice")
        query = sqla_select(model=User, loads=("posts",), query=base_query)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) == 1
        assert users[0].name == "alice"

    async def test_check_tables(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=("posts",), check_tables=True)
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0

    def test_declarative_base_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="must not be orm.DeclarativeBase"):
            sqla_select(model=orm.DeclarativeBase, self_key="")

    async def test_empty_loads(self, session: AsyncSession, seed_data: dict[str, list[Base]]) -> None:
        query = sqla_select(model=User, loads=())
        result = await session.execute(query)
        users = result.scalars().all()

        assert len(users) == 3

    @pytest.mark.lateral
    async def test_check_tables_with_o2m_lateral(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User, loads=("posts", "sent_messages"), check_tables=True
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0

    async def test_check_tables_with_m2m(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("roles",), check_tables=True)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.roles) > 0

    async def test_distinct_with_m2m(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("roles",), distinct=True)
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0

    async def test_distinct_with_deep_dotted_path(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User, loads=("posts.comments.reactions",), distinct=True
        )
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

    async def test_distinct_with_no_limit(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User, loads=("posts",), limit=None, distinct=True
        )
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "DISTINCT" in sql_text.upper()

        result = await session.execute(query)
        users = result.unique().scalars().all()

        assert len(users) > 0


    @pytest.mark.lateral
    async def test_check_tables_m2m_secondary_in_base(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        # check_tables=True when M2M secondary table is already joined in base query.
        base = sa.select(User).outerjoin(user_roles)
        query = sqla_select(
            model=User,
            loads=("roles",),
            query=base,
            check_tables=True,
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")
        assert len(alice.roles) > 0

    async def test_base_query_with_order_by(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """Base query with ORDER BY clause + relationship loads."""
        base = sa.select(User).order_by(User.name.asc())
        query = sqla_select(model=User, loads=("posts",), query=base)
        result = await session.execute(query)
        users = result.unique().scalars().all()
        names = [u.name for u in users]
        assert names == sorted(names)
        assert all(len(u.posts) >= 0 for u in users)


class TestCheckTablesNaming:

    @pytest.mark.lateral
    async def test_check_tables_user_multi_o2m_lateral_names(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("sent_messages", "received_messages", "owned_messages"),
            check_tables=True,
        )
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        # First FK → default table name; second/third → disambiguated
        assert "messages" in sql_text
        assert "messages_received_messages" in sql_text
        assert "messages_owned_messages" in sql_text

        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.sent_messages) == 2
        assert len(alice.received_messages) == 1

    @pytest.mark.lateral
    async def test_check_tables_self_ref_naming(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=Category,
            loads=("children",),
            check_tables=True,
        )
        sql_text = str(query.compile(compile_kwargs={"literal_binds": True}))

        assert "categories_children" in sql_text

        result = await session.execute(query)
        categories = result.unique().scalars().all()
        root = next(c for c in categories if c.name == "root")

        assert len(root.children) == 2

    @pytest.mark.lateral
    async def test_check_tables_alias_suffix_on_collision(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        """When base query already contains 'posts' table, LATERAL name
        collides and gets the '_alias' suffix (posts → posts_alias)."""
        base = sa.select(User).outerjoin(Post, User.id == Post.author_id)
        query = sqla_select(
            model=User, loads=("posts",), query=base, check_tables=True
        )

        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) > 0

    @pytest.mark.lateral
    async def test_conditions_with_check_tables(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("posts", "sent_messages"),
            check_tables=True,
            conditions={"posts": add_conditions(Post.title == "Alice Post 1")},
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()
        alice = next(u for u in users if u.name == "alice")

        assert len(alice.posts) == 1
        assert alice.posts[0].title == "Alice Post 1"
        assert len(alice.sent_messages) == 2


class TestExternalWhereWithLateral:

    @pytest.mark.lateral
    async def test_where_on_lateral_alias(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=User, loads=("posts",))
        query = query.where(sa.literal_column("posts.title") == "Alice Post 1")
        result = await session.execute(query)
        users = result.unique().scalars().all()

        # Only alice has a post with that title
        assert len(users) == 1
        assert users[0].name == "alice"

    @pytest.mark.lateral
    async def test_where_on_self_ref_lateral_alias(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(model=Category, loads=("children",))
        query = query.where(sa.literal_column("categories_children.name") == "child_1")
        result = await session.execute(query)
        categories = result.unique().scalars().all()

        # Only root has a child named child_1
        root = next((c for c in categories if c.name == "root"), None)
        assert root is not None
        assert len(root.children) >= 1
        assert any(c.name == "child_1" for c in root.children)

    @pytest.mark.lateral
    async def test_where_on_is_alias_lateral(
        self, session: AsyncSession, seed_data: dict[str, list[Base]]
    ) -> None:
        query = sqla_select(
            model=User,
            loads=("sent_messages", "received_messages"),
        )
        query = query.where(
            sa.literal_column("messages_received_messages.id").is_not(None)
        )
        result = await session.execute(query)
        users = result.unique().scalars().all()

        # Only users with received messages pass the filter
        for user in users:
            assert len(user.received_messages) > 0
