from typing import Optional, Union

from schemainspect import DBInspector, NullInspector, get_inspector
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session
from sqlbag import raw_execute

from .changes import Changes
from .statements import Statements


class Migration(object):
    """
    The main class of migra
    """  # _ what does it do?

    def __init__(
        self,
        x_from: Union[Connection, Session, DBInspector, NullInspector],
        x_target: Union[Connection, Session, DBInspector, NullInspector],
        schema: Optional[str] = None,
    ) -> None:  # suggestion: x_from -> "source", x_target -> "target"
        self.statements = Statements()
        self.changes = Changes(None, None)
        self.schema = schema
        if isinstance(x_from, DBInspector):
            self.changes.i_from = x_from
        else:
            self.changes.i_from = get_inspector(x_from, schema=schema)
            if x_from:
                self.s_from = x_from
        if isinstance(x_target, DBInspector):
            self.changes.i_target = x_target
        else:
            self.changes.i_target = get_inspector(x_target, schema=schema)
            if x_target:
                self.s_target = x_target

    def inspect_from(self) -> None:
        self.changes.i_from = get_inspector(self.s_from, schema=self.schema)

    def inspect_target(self) -> None:
        self.changes.i_target = get_inspector(self.s_target, schema=self.schema)

    def clear(self) -> None:
        "Empty this Migration's statements"
        self.statements = Statements()

    def apply(self) -> None:
        for stmt in self.statements:
            raw_execute(self.s_from, stmt)
        self.changes.i_from = get_inspector(self.s_from, schema=self.schema)
        safety_on = self.statements.safe
        self.clear()
        self.set_safety(safety_on)

    def add(self, statements: Statements) -> None:
        self.statements += statements

    def add_sql(self, sql: str) -> None:
        self.statements += Statements([sql])

    def set_safety(self, safety_on: bool) -> None:
        self.statements.safe = safety_on

    def add_extension_changes(self, creates: bool = True, drops: bool = True) -> None:
        if creates:
            self.add(self.changes.extensions(creations_only=True))
        if drops:
            self.add(self.changes.extensions(drops_only=True))

    def add_all_changes(self, privileges: bool = False) -> None:
        self.add(self.changes.schemas(creations_only=True))

        self.add(self.changes.extensions(creations_only=True))
        self.add(self.changes.collations(creations_only=True))
        self.add(self.changes.enums(creations_only=True, modifications=False))
        self.add(self.changes.sequences(creations_only=True))
        self.add(self.changes.triggers(drops_only=True))
        self.add(self.changes.rlspolicies(drops_only=True))
        if privileges:
            self.add(self.changes.privileges(drops_only=True))
        self.add(self.changes.non_pk_constraints(drops_only=True))
        self.add(self.changes.pk_constraints(drops_only=True))
        self.add(self.changes.indexes(drops_only=True))

        self.add(self.changes.selectables())

        self.add(self.changes.sequences(drops_only=True))
        self.add(self.changes.enums(drops_only=True, modifications=False))
        self.add(self.changes.extensions(drops_only=True))
        self.add(self.changes.indexes(creations_only=True))
        self.add(self.changes.pk_constraints(creations_only=True))
        self.add(self.changes.non_pk_constraints(creations_only=True))
        if privileges:
            self.add(self.changes.privileges(creations_only=True))
        self.add(self.changes.rlspolicies(creations_only=True))
        self.add(self.changes.triggers(creations_only=True))
        self.add(self.changes.collations(drops_only=True))
        self.add(self.changes.schemas(drops_only=True))

    @property
    def sql(self) -> str:
        return self.statements.sql
