"""Minimal models for sqla-autoloads examples."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import orm


class Base(orm.DeclarativeBase):
    pass


user_roles = sa.Table(
    "user_roles",
    Base.metadata,
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), primary_key=True),
    sa.Column("role_id", sa.Integer, sa.ForeignKey("roles.id"), primary_key=True),
)


class User(Base):
    __tablename__ = "users"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String(100))

    posts: orm.Mapped[list[Post]] = orm.relationship(back_populates="author", lazy="noload")
    roles: orm.Mapped[list[Role]] = orm.relationship(
        secondary=user_roles, back_populates="users", lazy="noload"
    )
    profile: orm.Mapped[Profile | None] = orm.relationship(
        uselist=False, back_populates="user", lazy="noload"
    )


class Post(Base):
    __tablename__ = "posts"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    title: orm.Mapped[str] = orm.mapped_column(sa.String(200))
    author_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("users.id"))

    author: orm.Mapped[User] = orm.relationship(back_populates="posts", lazy="noload")
    comments: orm.Mapped[list[Comment]] = orm.relationship(back_populates="post", lazy="noload")


class Comment(Base):
    __tablename__ = "comments"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    text: orm.Mapped[str] = orm.mapped_column(sa.Text)
    post_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("posts.id"))

    post: orm.Mapped[Post] = orm.relationship(back_populates="comments", lazy="noload")
    reactions: orm.Mapped[list[Reaction]] = orm.relationship(
        back_populates="comment", lazy="noload"
    )


class Reaction(Base):
    __tablename__ = "reactions"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    emoji: orm.Mapped[str] = orm.mapped_column(sa.String(10))
    comment_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("comments.id"))

    comment: orm.Mapped[Comment] = orm.relationship(back_populates="reactions", lazy="noload")


class Role(Base):
    __tablename__ = "roles"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String(50))
    level: orm.Mapped[int] = orm.mapped_column(default=0)

    users: orm.Mapped[list[User]] = orm.relationship(
        secondary=user_roles, back_populates="roles", lazy="noload"
    )


class Category(Base):
    __tablename__ = "categories"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String(100))
    parent_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.ForeignKey("categories.id"), nullable=True
    )

    parent: orm.Mapped[Category | None] = orm.relationship(
        back_populates="children",
        remote_side=[id],
        lazy="noload",
    )
    children: orm.Mapped[list[Category]] = orm.relationship(back_populates="parent", lazy="noload")


class Profile(Base):
    __tablename__ = "profiles"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    bio: orm.Mapped[str] = orm.mapped_column(sa.Text, default="")
    user_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("users.id"), unique=True)

    user: orm.Mapped[User] = orm.relationship(back_populates="profile", lazy="noload")
