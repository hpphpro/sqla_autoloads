from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import orm


class Base(orm.DeclarativeBase):
    pass

# this is not necessary you may create it with base for example:
# class UserRoles(Base):
#     __tablename__ = "user_roles"
#     user_id = sa.Column(sa.Integer, sa.ForeignKey("users.id"), primary_key=True)
#     role_id = sa.Column(sa.Integer, sa.ForeignKey("roles.id"), primary_key=True)
#     user = orm.relationship("User", back_populates="roles")
#     role = orm.relationship("Role", back_populates="users")

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
    active: orm.Mapped[bool] = orm.mapped_column(default=True)

    # relationships
    posts: orm.Mapped[list[Post]] = orm.relationship(
        back_populates="author", lazy="noload"
    )
    roles: orm.Mapped[list[Role]] = orm.relationship(
        secondary=user_roles, back_populates="users", lazy="noload"
    )
    sent_messages: orm.Mapped[list[Message]] = orm.relationship(
        foreign_keys="Message.from_user_id", back_populates="from_user", lazy="noload"
    )
    received_messages: orm.Mapped[list[Message]] = orm.relationship(
        foreign_keys="Message.to_user_id", back_populates="to_user", lazy="noload"
    )
    owned_messages: orm.Mapped[list[Message]] = orm.relationship(
        foreign_keys="Message.owner_id", back_populates="owner", lazy="noload"
    )
    profile: orm.Mapped[Profile | None] = orm.relationship(
        uselist=False, back_populates="user", lazy="noload"
    )


class Post(Base):
    __tablename__ = "posts"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    title: orm.Mapped[str] = orm.mapped_column(sa.String(200))
    body: orm.Mapped[str] = orm.mapped_column(sa.Text, default="")
    author_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("users.id"))

    author: orm.Mapped[User] = orm.relationship(back_populates="posts", lazy="noload")
    comments: orm.Mapped[list[Comment]] = orm.relationship(
        back_populates="post", lazy="noload"
    )
    # relationships
    tags: orm.Mapped[list[Tag]] = orm.relationship(
        secondary="post_tags",
        back_populates="posts",
        viewonly=True,
        lazy="noload",
    )
    post_tags: orm.Mapped[list[PostTag]] = orm.relationship(
        viewonly=True,
        primaryjoin="Post.id == foreign(PostTag.post_id)",
        lazy="noload",
    )
    attachments: orm.Mapped[list[Attachment]] = orm.relationship(
        primaryjoin="and_(Post.id == foreign(Attachment.attachable_id), Attachment.attachable_type == 'post')",
        viewonly=True,
        lazy="noload",
    )


class PostTag(Base):
    __tablename__ = "post_tags"

    post_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("posts.id"), primary_key=True
    )
    tag_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("tags.id"), primary_key=True
    )
    # relationships
    # viewonly=True because the Post <-> Tag link is already managed by the many-to-many
    # relationship using secondary="post_tags". These relationships on PostTag are for
    # read/navigation only. Without viewonly, sqlalchemy may try to manage the same
    # association via two paths (secondary M2M and the association object), which can
    # lead to overlap/conflict warnings and flush issues (duplicate INSERT/DELETE, etc.).
    # But since we still can load it, we can use viewonly=True
    post: orm.Mapped[Post] = orm.relationship(viewonly=True, lazy="noload")
    tag: orm.Mapped[Tag] = orm.relationship(viewonly=True, lazy="noload")


class Tag(Base):
    __tablename__ = "tags"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String(50))

    # relationships
    posts: orm.Mapped[list[Post]] = orm.relationship(
        secondary=PostTag.__table__, # or just "post_tags"
        back_populates="tags",
        lazy="noload",
    )
    post_tags: orm.Mapped[list[PostTag]] = orm.relationship(
        viewonly=True,
        primaryjoin="Tag.id == foreign(PostTag.tag_id)",
        lazy="noload",
    )


class Comment(Base):
    __tablename__ = "comments"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    text: orm.Mapped[str] = orm.mapped_column(sa.Text)
    post_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("posts.id"))

    # relationships
    post: orm.Mapped[Post] = orm.relationship(back_populates="comments", lazy="noload")
    reactions: orm.Mapped[list[Reaction]] = orm.relationship(
        back_populates="comment", lazy="noload"
    )
    attachments: orm.Mapped[list[Attachment]] = orm.relationship(
        primaryjoin="and_(Comment.id == foreign(Attachment.attachable_id), Attachment.attachable_type == 'comment')",
        lazy="noload",
    )


class Reaction(Base):
    __tablename__ = "reactions"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    emoji: orm.Mapped[str] = orm.mapped_column(sa.String(10))
    comment_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("comments.id"))
    # relationships
    comment: orm.Mapped[Comment] = orm.relationship(
        back_populates="reactions", lazy="noload"
    )


class Role(Base):
    __tablename__ = "roles"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String(50))
    level: orm.Mapped[int] = orm.mapped_column(default=0)

    # relationships
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

    # relationships
    parent: orm.Mapped[Category | None] = orm.relationship(
        back_populates="children", remote_side=[id], lazy="noload"
    )
    children: orm.Mapped[list[Category]] = orm.relationship(
        back_populates="parent", lazy="noload"
    )


class Message(Base):
    __tablename__ = "messages"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    content: orm.Mapped[str] = orm.mapped_column(sa.Text)
    from_user_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("users.id"))
    to_user_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("users.id"))
    owner_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("users.id"))

    # relationships
    # You may want to use primaryjoin here instead of foreign_keys
    from_user: orm.Mapped[User] = orm.relationship(
        foreign_keys=[from_user_id], back_populates="sent_messages", lazy="noload"
    )
    to_user: orm.Mapped[User] = orm.relationship(
        foreign_keys=[to_user_id], back_populates="received_messages", lazy="noload"
    )
    owner: orm.Mapped[User] = orm.relationship(
        foreign_keys=[owner_id], back_populates="owned_messages", lazy="noload"
    )


class Profile(Base):
    __tablename__ = "profiles"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    bio: orm.Mapped[str] = orm.mapped_column(sa.Text, default="")
    avatar_url: orm.Mapped[str] = orm.mapped_column(sa.String(500), default="")
    user_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("users.id"), unique=True
    )

    # relationships
    user: orm.Mapped[User] = orm.relationship(back_populates="profile", lazy="noload")


class Attachment(Base):
    __tablename__ = "attachments"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    url: orm.Mapped[str] = orm.mapped_column(sa.String(500))
    attachable_type: orm.Mapped[str] = orm.mapped_column(sa.String(50))
    attachable_id: orm.Mapped[int] = orm.mapped_column()
