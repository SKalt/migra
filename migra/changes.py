from collections import OrderedDict as od
from typing import Dict, Set

from schemainspect import DBInspector, Inspected

from .statements import Statements
from .util import differences

PK = "PRIMARY KEY"


def statements_for_changes(
    things_from: Dict[str, Inspected],
    things_target: Dict[str, Inspected],
    creations_only: bool = False,
    drops_only: bool = False,
    modifications: bool = True,
    dependency_ordering: bool = False,
    add_dependents_for_modified: bool = False,
) -> Statements:
    added, removed, modified, unmodified = differences(things_from, things_target)

    return statements_from_differences(
        added=added,
        removed=removed,
        modified=modified,
        replaceable=set(),
        creations_only=creations_only,
        drops_only=drops_only,
        modifications=modifications,
        dependency_ordering=dependency_ordering,
        old=things_from,
    )


def statements_from_differences(
    added: Dict[str, Inspected],
    removed: Dict[str, Inspected],
    modified: Dict[str, Inspected],
    replaceable: Set[str] = set(),
    creations_only: bool = False,
    drops_only: bool = False,
    modifications: bool = True,
    dependency_ordering: bool = False,
    old: Dict[str, Inspected] = {},
) -> Statements:
    replaceable, old = replaceable or set(), old or {}
    statements = Statements()
    if not creations_only:
        pending_drops = set(removed)
        if modifications:
            pending_drops |= set(modified) - replaceable
    else:
        pending_drops = set()
    if not drops_only:
        pending_creations = set(added)
        if modifications:
            pending_creations |= set(modified)
    else:
        pending_creations = set()

    def has_remaining_dependents(v: Inspected, pending_drops: Set[str]) -> bool:
        if not dependency_ordering:
            return False

        return bool(set(v.dependents) & pending_drops)

    def has_uncreated_dependencies(v: Inspected, pending_creations: Set[str]) -> bool:
        if not dependency_ordering:
            return False

        return bool(set(v.dependent_on) & pending_creations)

    while True:
        before = pending_drops | pending_creations
        if not creations_only:
            for k, v in removed.items():
                if not has_remaining_dependents(v, pending_drops):
                    if k in pending_drops:
                        statements.append(old[k].drop_statement)
                        pending_drops.remove(k)
        if not drops_only:
            for k, v in added.items():
                if not has_uncreated_dependencies(v, pending_creations):
                    if k in pending_creations:
                        statements.append(v.create_statement)
                        pending_creations.remove(k)
        if modifications:
            for k, v in modified.items():
                if not creations_only:
                    if not has_remaining_dependents(v, pending_drops):
                        if k in pending_drops:
                            statements.append(old[k].drop_statement)
                            pending_drops.remove(k)
                if not drops_only:
                    if not has_uncreated_dependencies(v, pending_creations):
                        if k in pending_creations:
                            statements.append(v.create_statement)
                            pending_creations.remove(k)
        after = pending_drops | pending_creations
        if not after:
            break

        elif (
            after == before
        ):  # this should never happen because there shouldn't be circular dependencies
            raise ValueError("cannot resolve dependencies")  # pragma: no cover
            # _ could use more debug info

    return statements


def get_enum_modifications(
    tables_from: Dict[str, Inspected],
    tables_target: Dict[str, Inspected],
    enums_from: Dict[str, Inspected],
    enums_target: Dict[str, Inspected],
) -> Statements:
    enums_modified: Dict[str, Inspected] = differences(enums_from, enums_target)[2]
    tables_modified: Dict[str, Inspected] = differences(tables_from, tables_target)[2]
    pre, recreate, post = Statements(), Statements(), Statements()

    enums_to_change = enums_modified
    for table, v in tables_modified.items():
        t_before = tables_from[table]
        c_modified = differences(t_before.columns, v.columns)[2]
        for k, c in c_modified.items():
            before = t_before.columns[k]
            if (
                c.is_enum == before.is_enum
                and c.dbtypestr == before.dbtypestr
                and c.enum != before.enum
            ):
                pre.append(before.change_enum_to_string_statement(table))
                post.append(before.change_string_to_enum_statement(table))
    for e in enums_to_change.values():
        recreate.append(e.drop_statement)
        recreate.append(e.create_statement)
    return pre + recreate + post


def get_table_changes(
    tables_from: Dict[str, Inspected],
    tables_target: Dict[str, Inspected],
    enums_from: Dict[str, Inspected],
    enums_target: Dict[str, Inspected],
) -> Statements:
    added, removed, modified, _ = differences(tables_from, tables_target)

    statements = Statements()
    for t, v in removed.items():
        statements.append(v.drop_statement)
    for t, v in added.items():
        statements.append(v.create_statement)
    statements += get_enum_modifications(
        tables_from, tables_target, enums_from, enums_target
    )

    for t, v in modified.items():
        before = tables_from[t]

        # drop/recreate tables which have changed from partitioned to non-partitioned
        if v.is_partitioned != before.is_partitioned:
            statements.append(v.drop_statement)
            statements.append(v.create_statement)
            continue

        # attach/detach tables with changed parent tables
        if v.parent_table != before.parent_table:
            statements += v.attach_detach_statements(before)

    for t, v in modified.items():
        before = tables_from[t]

        if not v.is_alterable:
            continue

        c_added, c_removed, c_modified, _ = differences(before.columns, v.columns)
        for k, c in c_removed.items():
            alter = v.alter_table_statement(c.drop_column_clause)
            statements.append(alter)
        for k, c in c_added.items():
            alter = v.alter_table_statement(c.add_column_clause)
            statements.append(alter)
        for k, c in c_modified.items():
            statements += c.alter_table_statements(before.columns[k], t)

        if v.rowsecurity != before.rowsecurity:
            rls_alter = v.alter_rls_statement
            statements += [rls_alter]
    return statements


def get_selectable_changes(
    selectables_from: Inspected,
    selectables_target: Inspected,
    enums_from: Dict[str, Inspected],
    enums_target: Dict[str, Inspected],
    add_dependents_for_modified: bool = True,
) -> Statements:
    tables_from = od((k, v) for k, v in selectables_from.items() if v.is_table)
    tables_target = od((k, v) for k, v in selectables_target.items() if v.is_table)

    other_from = od((k, v) for k, v in selectables_from.items() if not v.is_table)
    other_target = od((k, v) for k, v in selectables_target.items() if not v.is_table)

    added_tables, removed_tables, modified_tables, unmodified_tables = differences(
        tables_from, tables_target
    )
    added_other, removed_other, modified_other, unmodified_other = differences(
        other_from, other_target
    )

    changed_all = {}
    changed_all.update(modified_tables)
    changed_all.update(modified_other)
    modified_all = dict(changed_all)
    changed_all.update(removed_tables)
    changed_all.update(removed_other)

    replaceable = set()
    not_replaceable = set()

    if add_dependents_for_modified:
        for k, m in changed_all.items():
            old = selectables_from[k]

            if k in modified_all and m.can_replace(old):
                if not m.is_table:
                    replaceable.add(k)
                continue

            for d in m.dependents_all:
                if d in unmodified_other:
                    dd = unmodified_other.pop(d)
                    modified_other[d] = dd
                not_replaceable.add(d)
        modified_other = od(sorted(modified_other.items()))

    replaceable -= not_replaceable
    statements = Statements()

    def functions(d: Dict[str, Inspected]) -> Dict[str, Inspected]:
        return {k: v for k, v in d.items() if v.relationtype == "f"}

    statements += statements_from_differences(
        added_other,
        removed_other,
        modified_other,
        replaceable=replaceable,
        drops_only=True,
        dependency_ordering=True,
        old=selectables_from,
    )

    statements += get_table_changes(
        tables_from, tables_target, enums_from, enums_target
    )

    if any([functions(added_other), functions(modified_other)]):
        statements += ["set check_function_bodies = off;"]

    statements += statements_from_differences(
        added_other,
        removed_other,
        modified_other,
        replaceable=replaceable,
        creations_only=True,
        dependency_ordering=True,
        old=selectables_from,
    )
    return statements


class Changes(object):
    def __init__(self, i_from: DBInspector, i_target: DBInspector) -> None:
        self.i_from = i_from
        self.i_target = i_target

    def non_pk_constraints(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        a, b = (i.constraints.items() for i in (self.i_from, self.i_target))
        a_od = dict((k, v) for k, v in a if v.constraint_type != PK)
        b_od = dict((k, v) for k, v in b if v.constraint_type != PK)
        return statements_for_changes(
            a_od,
            b_od,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def pk_constraints(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        a, b = (i.constraints.items() for i in (self.i_from, self.i_target))
        a_od = od((k, v) for k, v in a if v.constraint_type == PK)
        b_od = od((k, v) for k, v in b if v.constraint_type == PK)
        return statements_for_changes(
            a_od,
            b_od,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def selectables(self, add_dependents_for_modified: bool = True,) -> Statements:
        return get_selectable_changes(
            od(sorted(self.i_from.selectables.items())),
            od(sorted(self.i_target.selectables.items())),
            self.i_from.enums,
            self.i_target.enums,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def schemas(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.schemas,
            self.i_target.schemas,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def enums(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.enums,
            self.i_target.enums,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def sequences(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.sequences,
            self.i_target.sequences,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def constraints(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.constraints,
            self.i_target.constraints,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def functions(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from,
            self.i_target,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def views(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.views,
            self.i_target.views,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def indexes(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.indexes,
            self.i_target.indexes,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def extensions(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.extensions,
            self.i_target.extensions,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def privileges(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.privileges,
            self.i_target.privileges,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def collations(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.collations,
            self.i_target.collations,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def rlspolicies(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.rlspolicies,
            self.i_target.rlspolicies,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )

    def triggers(
        self,
        creations_only: bool = False,
        drops_only: bool = False,
        modifications: bool = True,
        dependency_ordering: bool = False,
        add_dependents_for_modified: bool = False,
    ) -> Statements:
        return statements_for_changes(
            self.i_from.triggers,
            self.i_target.triggers,
            creations_only=creations_only,
            drops_only=drops_only,
            modifications=modifications,
            dependency_ordering=dependency_ordering,
            add_dependents_for_modified=add_dependents_for_modified,
        )
