import re
from typing import AnyStr


def check_for_drop(s: AnyStr) -> bool:
    return bool(re.search(r"(drop\s+)", s, re.IGNORECASE))


class Statements(list):
    # TODO: docstring here
    def __init__(self, *args, **kwargs):  # _  super to list does not need **kwargs
        # TODO: what types can/are passed into this modified list?
        self.safe = True
        super(Statements, self).__init__(*args, **kwargs)

    @property
    def sql(self):
        if self.safe:
            self.raise_if_unsafe()
        if not self:  # _ shouldn't this come before `if self.safe`?
            return ""

        return "\n\n".join(self) + "\n\n"

    def raise_if_unsafe(self):
        if any(check_for_drop(s) for s in self):
            raise UnsafeMigrationException(
                "unsafe/destructive change being autogenerated, refusing to carry on further"
            ) # _ might be able to use more debug info here

    def __add__(self, other):
        self += list(other)
        return self


class UnsafeMigrationException(Exception):
    pass
