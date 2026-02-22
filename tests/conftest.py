from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from typing import Final

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)

from sqla_autoloads import sqla_cache_clear
from sqla_autoloads.node import Node, get_node, init_node

from .models import (
    Attachment,
    Base,
    Category,
    Comment,
    Message,
    Post,
    PostTag,
    Profile,
    Reaction,
    Role,
    Tag,
    User,
    user_roles,
)


LATERAL_BACKENDS: Final[frozenset[str]] = frozenset({"postgres", "mysql"})

pytestmark = pytest.mark.anyio


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--db",
        default="postgres",
        choices=["postgres", "mysql", "mariadb", "sqlite"],
        help="Database backend to test against",
    )


@pytest.fixture(scope="session")
def db_backend(request: pytest.FixtureRequest) -> str:
    value: str = request.config.getoption("--db")

    return value


@pytest.fixture(scope="session")
def supports_lateral(db_backend: str) -> bool:
    return db_backend in LATERAL_BACKENDS


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session", autouse=True)
def _init_node() -> None:
    """Initialize the Node singleton with model relationships.

    Sync, no DB needed -- safe to run for all tests including unit tests.
    """
    try:
        Node()
    except RuntimeError:
        Node.reset()
        init_node(get_node(Base))


@pytest.fixture(scope="session")
def db_config(db_backend: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    match db_backend:
        case "postgres":
            from testcontainers.postgres import PostgresContainer

            pg = PostgresContainer(image="postgres:latest")
            if os.name == "nt":
                pg.get_container_host_ip = lambda: "127.0.0.1"
            with pg:
                host = pg.get_container_host_ip()
                dsn = (
                    f"postgresql+asyncpg://{pg.username}:{pg.password}"
                    f"@{host}:{pg.get_exposed_port(pg.port)}/{pg.dbname}"
                )
                yield dsn

        case "mysql":
            from testcontainers.mysql import MySqlContainer

            my = MySqlContainer(image="mysql:8.0")
            if os.name == "nt":
                my.get_container_host_ip = lambda: "127.0.0.1"
            with my:
                host = my.get_container_host_ip()
                port = my.get_exposed_port(my.port)
                dsn = (
                    f"mysql+asyncmy://{my.username}:{my.password}"
                    f"@{host}:{port}/{my.dbname}"
                )
                yield dsn

        case "mariadb":
            from testcontainers.mysql import MySqlContainer as MariaDBContainer

            ma = MariaDBContainer(image="mariadb:latest")
            if os.name == "nt":
                ma.get_container_host_ip = lambda: "127.0.0.1"
            with ma:
                host = ma.get_container_host_ip()
                port = ma.get_exposed_port(ma.port)
                dsn = (
                    f"mysql+asyncmy://{ma.username}:{ma.password}"
                    f"@{host}:{port}/{ma.dbname}"
                )
                yield dsn

        case "sqlite":
            tmp = tmp_path_factory.mktemp("db")
            yield f"sqlite+aiosqlite:///{tmp}/test.db"


@pytest.fixture(scope="session")
def engine(db_config: str) -> AsyncEngine:
    return create_async_engine(db_config, echo=False)


@pytest.fixture(scope="session")
async def _create_tables(engine: AsyncEngine) -> AsyncIterator[None]:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def connection(
    engine: AsyncEngine, _create_tables: None
) -> AsyncIterator[AsyncConnection]:
    async with engine.connect() as conn:
        trans = await conn.begin()
        yield conn
        await trans.rollback()


@pytest.fixture
async def session(connection: AsyncConnection) -> AsyncIterator[AsyncSession]:
    sess = AsyncSession(bind=connection, expire_on_commit=False)
    yield sess
    await sess.close()


@pytest.fixture
async def seed_data(session: AsyncSession) -> dict[str, list[Base]]:
    alice = User(id=1, name="alice", active=True)
    bob = User(id=2, name="bob", active=True)
    charlie = User(id=3, name="charlie", active=False)
    session.add_all([alice, bob, charlie])
    await session.flush()

    post1 = Post(id=1, title="Alice Post 1", body="body1", author_id=1)
    post2 = Post(id=2, title="Alice Post 2", body="body2", author_id=1)
    post3 = Post(id=3, title="Alice Post 3", body="body3", author_id=1)
    post4 = Post(id=4, title="Bob Post 1", body="body4", author_id=2)
    session.add_all([post1, post2, post3, post4])
    await session.flush()

    tag_python = Tag(id=1, name="python")
    tag_sqlalchemy = Tag(id=2, name="sqlalchemy")
    tag_testing = Tag(id=3, name="testing")
    session.add_all([tag_python, tag_sqlalchemy, tag_testing])
    await session.flush()

    session.add_all([
        PostTag(post_id=1, tag_id=1),
        PostTag(post_id=1, tag_id=2),
        PostTag(post_id=2, tag_id=1),
        PostTag(post_id=4, tag_id=3),
    ])
    await session.flush()

    comment1 = Comment(id=1, text="Great post!", post_id=1)
    comment2 = Comment(id=2, text="Nice work", post_id=1)
    session.add_all([comment1, comment2])
    await session.flush()

    reaction1 = Reaction(id=1, emoji="\U0001f44d", comment_id=1)
    reaction2 = Reaction(id=2, emoji="\u2764\ufe0f", comment_id=1)
    session.add_all([reaction1, reaction2])
    await session.flush()

    admin = Role(id=1, name="admin", level=10)
    editor = Role(id=2, name="editor", level=5)
    viewer = Role(id=3, name="viewer", level=1)
    session.add_all([admin, editor, viewer])
    await session.flush()

    await session.execute(
        user_roles.insert().values([
            {"user_id": 1, "role_id": 1},
            {"user_id": 1, "role_id": 2},
            {"user_id": 2, "role_id": 2},
            {"user_id": 2, "role_id": 3},
        ])
    )
    await session.flush()

    root = Category(id=1, name="root", parent_id=None)
    child1 = Category(id=2, name="child_1", parent_id=1)
    child2 = Category(id=3, name="child_2", parent_id=1)
    grandchild = Category(id=4, name="grandchild", parent_id=2)
    session.add_all([root, child1, child2, grandchild])
    await session.flush()

    msg1 = Message(id=1, content="Hello Bob", from_user_id=1, to_user_id=2, owner_id=1)
    msg2 = Message(id=2, content="Hi Alice", from_user_id=2, to_user_id=1, owner_id=2)
    msg3 = Message(id=3, content="Hey Charlie", from_user_id=1, to_user_id=3, owner_id=3)
    session.add_all([msg1, msg2, msg3])
    await session.flush()

    profile_alice = Profile(id=1, bio="Alice bio", avatar_url="https://example.com/alice.jpg", user_id=1)
    profile_bob = Profile(id=2, bio="Bob bio", avatar_url="https://example.com/bob.jpg", user_id=2)
    session.add_all([profile_alice, profile_bob])
    await session.flush()

    att1 = Attachment(id=1, url="https://example.com/post1_img1.jpg", attachable_type="post", attachable_id=1)
    att2 = Attachment(id=2, url="https://example.com/post1_img2.jpg", attachable_type="post", attachable_id=1)
    att3 = Attachment(id=3, url="https://example.com/comment1_file.pdf", attachable_type="comment", attachable_id=1)
    session.add_all([att1, att2, att3])
    await session.flush()

    session.expunge_all()

    return {
        "users": [alice, bob, charlie],
        "posts": [post1, post2, post3, post4],
        "comments": [comment1, comment2],
        "reactions": [reaction1, reaction2],
        "roles": [admin, editor, viewer],
        "categories": [root, child1, child2, grandchild],
        "messages": [msg1, msg2, msg3],
        "tags": [tag_python, tag_sqlalchemy, tag_testing],
        "profiles": [profile_alice, profile_bob],
        "attachments": [att1, att2, att3],
    }


@pytest.fixture(autouse=True)
def clear_lru_caches() -> Iterator[None]:
    yield
    sqla_cache_clear()


@pytest.fixture
def reset_node_singleton() -> Iterator[None]:
    saved = Node._Node__instance  # type: ignore[attr-defined]
    yield
    Node._Node__instance = saved  # type: ignore[attr-defined]


# Multi-dialect: auto-skip @pytest.mark.lateral on non-lateral backends

@pytest.fixture(autouse=True)
def _skip_lateral(request: pytest.FixtureRequest, supports_lateral: bool) -> None:
    if request.node.get_closest_marker("lateral") and not supports_lateral:
        pytest.skip("LATERAL not supported on this backend")


# Multi-dialect: override default limit on non-lateral backends


@pytest.fixture(scope="session", autouse=True)
def _patch_default_limit(supports_lateral: bool) -> Iterator[None]:
    if not supports_lateral:
        import sqla_autoloads.core as core

        original = core.DEFAULT_RELATIONSHIP_LOAD_LIMIT
        core.DEFAULT_RELATIONSHIP_LOAD_LIMIT = None  # type: ignore[misc, assignment]
        yield
        core.DEFAULT_RELATIONSHIP_LOAD_LIMIT = original  # type: ignore[misc]
    else:
        yield
