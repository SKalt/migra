from typing import Dict, Hashable, Set, Tuple, TypeVar

K = TypeVar("K", Hashable, str)
V = TypeVar("V")


def _added(a: Dict[K, V], b: Dict[K, V], a_keys: Set[K], b_keys: Set[K]) -> Dict[K, V]:
    return {k: b[k] for k in sorted(b_keys - a_keys)}


def _removed(
    a: Dict[K, V], b: Dict[K, V], a_keys: Set[K], b_keys: Set[K]
) -> Dict[K, V]:
    return {k: a[k] for k in sorted(a_keys - b_keys)}


def _differences(
    a: Dict[K, V], b: Dict[K, V], a_keys: Set[K], b_keys: Set[K]
) -> Tuple[Dict[K, V], Dict[K, V]]:
    common_keys = a_keys.intersection(b_keys)
    modified, unmodified = {}, {}  # type: Dict[K, V], Dict[K, V]
    for key in common_keys:
        old_value, new_value = a[key], b[key]
        if new_value == old_value:
            unmodified[key] = new_value
        else:
            modified[key] = new_value
    return modified, unmodified


def differences(
    a: Dict[K, V], b: Dict[K, V], add_dependencies_for_modifications: bool = True
) -> Tuple[Dict[K, V], Dict[K, V], Dict[K, V], Dict[K, V]]:
    a_keys, b_keys = set(a.keys()), set(b.keys())
    added, removed = _added(a, b, a_keys, b_keys), _removed(a, b, a_keys, b_keys)
    modified, unmodified = _differences(a, b, a_keys, b_keys)
    return added, removed, modified, unmodified
