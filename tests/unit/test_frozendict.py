from __future__ import annotations

import pytest

from sqla_autoloads.datastructures import frozendict


class TestFrozendictInit:
    def test_from_dict(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1, "b": 2})
        assert fd["a"] == 1
        assert fd["b"] == 2

    def test_from_kwargs(self) -> None:
        fd: frozendict[str, int] = frozendict(x=10, y=20)
        assert fd["x"] == 10

    def test_empty(self) -> None:
        fd: frozendict[str, int] = frozendict()
        assert len(fd) == 0

    def test_from_pairs(self) -> None:
        fd: frozendict[str, str] = frozendict([("k1", "v1"), ("k2", "v2")])
        assert fd["k1"] == "v1"


class TestFrozendictImmutability:
    def test_no_setitem(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        with pytest.raises(TypeError):
            fd["a"] = 2  # type: ignore[index]

    def test_no_delitem(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        with pytest.raises(TypeError):
            del fd["a"]  # type: ignore[attr-defined]


class TestFrozendictHash:
    def test_hashable(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        assert isinstance(hash(fd), int)

    def test_equal_dicts_same_hash(self) -> None:
        fd1: frozendict[str, int] = frozendict({"a": 1, "b": 2})
        fd2: frozendict[str, int] = frozendict({"b": 2, "a": 1})
        assert hash(fd1) == hash(fd2)

    def test_usable_as_dict_key(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        d = {fd: "value"}
        assert d[fd] == "value"

    def test_usable_in_set(self) -> None:
        fd1: frozendict[str, int] = frozendict({"a": 1})
        fd2: frozendict[str, int] = frozendict({"a": 1})
        s: set[frozendict[str, int]] = {fd1, fd2}
        assert len(s) == 1


class TestFrozendictEquality:
    def test_equal_frozendicts(self) -> None:
        fd1: frozendict[str, int] = frozendict({"a": 1})
        fd2: frozendict[str, int] = frozendict({"a": 1})
        assert fd1 == fd2

    def test_not_equal_frozendicts(self) -> None:
        fd1: frozendict[str, int] = frozendict({"a": 1})
        fd2: frozendict[str, int] = frozendict({"a": 2})
        assert fd1 != fd2

    def test_equal_to_dict(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1, "b": 2})
        assert fd == {"a": 1, "b": 2}

    def test_not_equal_to_other_types(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        assert fd != [("a", 1)]


class TestFrozendictCopy:
    def test_copy_creates_new_instance(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        fd2: frozendict[str, int] = fd.copy(b=2)
        assert fd2["a"] == 1
        assert fd2["b"] == 2
        assert "b" not in fd

    def test_copy_replaces(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        fd2: frozendict[str, int] = fd.copy(a=99)
        assert fd2["a"] == 99
        assert fd["a"] == 1


class TestFrozendictIteration:
    def test_iter_keys(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1, "b": 2})
        assert set(fd) == {"a", "b"}

    def test_len(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1, "b": 2, "c": 3})
        assert len(fd) == 3

    def test_contains(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        assert "a" in fd
        assert "z" not in fd


class TestFrozendictRepr:
    def test_repr_format(self) -> None:
        fd: frozendict[str, int] = frozendict({"a": 1})
        r = repr(fd)
        assert r.startswith("<frozendict")
        assert "'a': 1" in r
