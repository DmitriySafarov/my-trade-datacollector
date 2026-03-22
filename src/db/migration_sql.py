from __future__ import annotations


def split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: str | None = None
    index = 0
    length = len(sql)

    while index < length:
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < length else ""

        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        if dollar_tag is not None:
            if sql.startswith(dollar_tag, index):
                current.append(dollar_tag)
                index += len(dollar_tag)
                dollar_tag = None
                continue
            current.append(char)
            index += 1
            continue

        if in_single:
            current.append(char)
            if char == "'" and next_char == "'":
                current.append(next_char)
                index += 2
                continue
            if char == "'":
                in_single = False
            index += 1
            continue

        if in_double:
            current.append(char)
            if char == '"' and next_char == '"':
                current.append(next_char)
                index += 2
                continue
            if char == '"':
                in_double = False
            index += 1
            continue

        if char == "-" and next_char == "-":
            current.extend([char, next_char])
            in_line_comment = True
            index += 2
            continue

        if char == "/" and next_char == "*":
            current.extend([char, next_char])
            in_block_comment = True
            index += 2
            continue

        if char == "'":
            current.append(char)
            in_single = True
            index += 1
            continue

        if char == '"':
            current.append(char)
            in_double = True
            index += 1
            continue

        if char == "$":
            tag = _read_dollar_tag(sql, index)
            if tag is not None:
                current.append(tag)
                dollar_tag = tag
                index += len(tag)
                continue

        if char == ";":
            current.append(char)
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


def _read_dollar_tag(sql: str, start: int) -> str | None:
    end = start + 1
    while end < len(sql):
        char = sql[end]
        if char == "$":
            return sql[start : end + 1]
        if not (char == "_" or char.isalnum()):
            return None
        end += 1
    return None
