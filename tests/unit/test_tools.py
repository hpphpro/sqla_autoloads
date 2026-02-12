from __future__ import annotations


import sqlalchemy as sa
from sqlalchemy import orm

from sqla_autoloads.tools import add_conditions, get_primary_key, get_table_name, get_table_names

from ..models import Category, Comment, Post, Role, User


class TestGetTableName:
    def test_user_table_name(self) -> None:
        assert get_table_name(User) == "users"

    def test_post_table_name(self) -> None:
        assert get_table_name(Post) == "posts"

    def test_category_table_name(self) -> None:
        assert get_table_name(Category) == "categories"


class TestGetPrimaryKey:
    def test_user_pk(self) -> None:
        pk = get_primary_key(User)
        assert pk.name == "id"

    def test_post_pk(self) -> None:
        pk = get_primary_key(Post)
        assert pk.name == "id"


class TestGetTableNames:
    def test_simple_select(self) -> None:
        q = sa.select(User)
        names = get_table_names(q)
        assert "users" in names

    def test_join_query(self) -> None:
        q = sa.select(User).outerjoin(Post, User.id == Post.author_id)
        names = get_table_names(q)
        assert "users" in names
        assert "posts" in names

    def test_alias_from_table(self) -> None:
        alias = orm.aliased(Post, name="posts_alias")
        q = sa.select(User).outerjoin(alias, sa.true())
        names = get_table_names(q)
        assert "users" in names
        assert "posts" in names

    def test_multiple_joins(self) -> None:
        q = (
            sa.select(User)
            .outerjoin(Post, User.id == Post.author_id)
            .outerjoin(Comment, Post.id == Comment.post_id)
        )
        names = get_table_names(q)
        assert "users" in names
        assert "posts" in names
        assert "comments" in names


class TestAddConditions:
    def test_single_condition(self) -> None:
        cond_fn = add_conditions(Role.level > 3)
        q = sa.select(Role)
        q2 = cond_fn(q)
        compiled = str(q2.compile(compile_kwargs={"literal_binds": True}))
        assert "level > 3" in compiled

    def test_multiple_conditions(self) -> None:
        cond_fn = add_conditions(Role.level > 3, Role.name == "admin")
        q = sa.select(Role)
        q2 = cond_fn(q)
        compiled = str(q2.compile(compile_kwargs={"literal_binds": True}))
        assert "level > 3" in compiled
        assert "admin" in compiled

    def test_returns_callable(self) -> None:
        cond_fn = add_conditions(User.active.is_(True))
        assert callable(cond_fn)
