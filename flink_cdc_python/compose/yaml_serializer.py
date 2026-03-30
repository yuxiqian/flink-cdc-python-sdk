"""
YAML serialization for Flink CDC pipeline definitions.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any


def to_yaml(data: dict[str, Any] | object, indent: int = 2) -> str:
    """
    Convert a dictionary or object to YAML string.

    This is a lightweight YAML serializer that doesn't require external dependencies.
    For more complex YAML needs, consider installing PyYAML.

    Args:
        data: Dictionary or object with to_dict() method to convert
        indent: Indentation spaces (default: 2)

    Returns:
        YAML string representation
    """
    if hasattr(data, 'to_dict'):
        data = data.to_dict()

    if not isinstance(data, dict):
        raise TypeError(f'Expected dict or object with to_dict method, got {type(data)}')

    output = StringIO()
    _write_dict(output, data, indent=indent, level=0)
    return output.getvalue().rstrip() + '\n'


def _needs_quoting(value: str) -> bool:
    """Check if a string value needs to be quoted in YAML."""
    if not value:
        return True

    # Characters that require quoting
    special_chars = {'!', '&', '*', '{', '}', '[', ']', '|', '>', "'", '"', '%', '@', '`'}
    if value[0] in special_chars:
        return True

    # Check for colon followed by space or end of string (YAML key-value indicator)
    if ': ' in value or value.endswith(':'):
        return True

    # Check for special YAML values
    lower_value = value.lower()
    if lower_value in ('true', 'false', 'null', '~', 'yes', 'no', 'on', 'off'):
        return True

    # Check if it starts with a number (could be interpreted as numeric)
    if value[0].isdigit() and not value.replace('.', '').replace('-', '').isalnum():
        return True

    return False


def _write_dict(
    output: StringIO,
    data: dict[str, Any],
    indent: int,
    level: int,
) -> None:
    """Write a dictionary to the output buffer."""
    prefix = ' ' * (indent * level)

    for key, value in data.items():
        _write_key_value(output, key, value, indent, level, prefix)


def _write_key_value(
    output: StringIO,
    key: str,
    value: Any,
    indent: int,
    level: int,
    prefix: str,
) -> None:
    """Write a key-value pair to the output buffer."""
    if value is None:
        # Skip None values
        return

    if isinstance(value, bool):
        yaml_value = 'true' if value else 'false'
        output.write(f'{prefix}{key}: {yaml_value}\n')

    elif isinstance(value, (int, float)):
        output.write(f'{prefix}{key}: {value}\n')

    elif isinstance(value, str):
        # Handle multi-line strings
        if '\n' in value:
            output.write(f'{prefix}{key}: |\n')
            for line in value.split('\n'):
                output.write(f'{prefix}{" " * indent}{line}\n')
        elif _needs_quoting(value):
            # Use single quotes, escape internal single quotes
            escaped = value.replace("'", "''")
            output.write(f"{prefix}{key}: '{escaped}'\n")
        else:
            output.write(f'{prefix}{key}: {value}\n')

    elif isinstance(value, list):
        if not value:
            output.write(f'{prefix}{key}: []\n')
        else:
            output.write(f'{prefix}{key}:\n')
            _write_list(output, value, indent, level + 1)

    elif isinstance(value, dict):
        if not value:
            output.write(f'{prefix}{key}: {{}}\n')
        else:
            output.write(f'{prefix}{key}:\n')
            _write_dict(output, value, indent, level + 1)

    else:
        # Fallback: convert to string
        output.write(f'{prefix}{key}: {str(value)}\n')


def _write_list(output: StringIO, data: list[Any], indent: int, level: int) -> None:
    """Write a list to the output buffer."""
    prefix = ' ' * (indent * level)

    for item in data:
        if isinstance(item, dict):
            # First key goes on the same line as the dash
            items = list(item.items())
            if items:
                first_key, first_value = items[0]
                output.write(f'{prefix}- {first_key}: ')

                if isinstance(first_value, dict) and first_value:
                    output.write('\n')
                    _write_dict(output, first_value, indent, level + 2)
                elif isinstance(first_value, list) and first_value:
                    output.write('\n')
                    _write_list(output, first_value, indent, level + 2)
                elif isinstance(first_value, str):
                    if '\n' in first_value:
                        output.write('|\n')
                        for line in first_value.split('\n'):
                            output.write(f'{prefix}{" " * indent * 2}{line}\n')
                    elif _needs_quoting(first_value):
                        escaped = first_value.replace("'", "''")
                        output.write(f"'{escaped}'\n")
                    else:
                        output.write(f'{first_value}\n')
                elif isinstance(first_value, bool):
                    output.write(f'{"true" if first_value else "false"}\n')
                elif first_value is None:
                    output.write('null\n')
                else:
                    output.write(f'{first_value}\n')

                # Write remaining keys with proper indentation
                for key, value in items[1:]:
                    item_prefix = prefix + ' ' * indent
                    _write_key_value(output, key, value, indent, level + 1, item_prefix)

        elif isinstance(item, str):
            if '\n' in item:
                output.write(f'{prefix}- |\n')
                for line in item.split('\n'):
                    output.write(f'{prefix}{" " * indent}{line}\n')
            elif _needs_quoting(item):
                escaped = item.replace("'", "''")
                output.write(f"{prefix}- '{escaped}'\n")
            else:
                output.write(f'{prefix}- {item}\n')

        elif isinstance(item, (int, float)):
            output.write(f'{prefix}- {item}\n')

        elif isinstance(item, bool):
            output.write(f'{prefix}- {"true" if item else "false"}\n')

        elif item is None:
            output.write(f'{prefix}- null\n')

        else:
            output.write(f'{prefix}- {str(item)}\n')


def save_yaml(data: dict[str, Any] | object, file_path: str | Path, indent: int = 2) -> str:
    """
    Save YAML to a file.

    Args:
        data: Dictionary or object with to_dict() method
        file_path: Path to the output file
        indent: Indentation spaces

    Returns:
        The file path as string
    """
    yaml_content = to_yaml(data, indent)
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(yaml_content)
    return str(path)
