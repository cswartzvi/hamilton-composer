"""Simple Hamilton test nodes for basic math operations."""

from typing import Any, Dict, List

from hamilton.function_modifiers import config


def doubled_numbers(numbers: List[int]) -> List[int]:
    """Double each number in the list."""
    result = [n * 2 for n in numbers]
    print(f"doubled_numbers = {result}")
    return result


def sum_doubled(doubled_numbers: List[int]) -> int:
    """Sum of all doubled numbers."""
    result = sum(doubled_numbers)
    print(f"sum_doubled = {result}")
    return result


@config.when(method="add")
def transformed_sum__add(sum_doubled: int, factor: int) -> int:
    """Add factor to sum."""
    result = sum_doubled + factor
    print(f"transformed_sum__add = {result}")
    return result


@config.when(method="multiply")
def transformed_sum__multiply(sum_doubled: int, factor: int) -> int:
    """Multiply sum by factor."""
    result = sum_doubled * factor
    print(f"transformed_sum__multiply = {result}")
    return result
