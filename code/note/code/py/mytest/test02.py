import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Punctuation, Name, Whitespace


def extract_tables(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    tables = set()
    parsed = sqlparse.parse(sql_content)
    for statement in parsed:
        tokens = iter(statement.tokens)
        while True:
            try:
                token = next(tokens)
            except StopIteration:
                break

            if token.ttype is Keyword:
                if token.value.upper() == 'FROM':
                    tables.update(extract_tables_from_from_clause(tokens))
                elif token.value.upper().startswith('JOIN'):
                    tables.update(extract_tables_from_join_clause(tokens))

    return tables


def extract_tables_from_from_clause(tokens):
    tables = set()
    while True:
        try:
            token = next(tokens)
        except StopIteration:
            break

        if token.ttype is Name or isinstance(token, (Identifier, IdentifierList)):
            tables.add(token.get_real_name() if isinstance(token, IdentifierList) else token.value)
        elif token.ttype in (Keyword, Punctuation):
            break

    return tables


def extract_tables_from_join_clause(tokens):
    tables = set()
    # Skip 'JOIN' and optional keywords like 'INNER', 'LEFT', etc.
    while True:
        try:
            token = next(tokens)
        except StopIteration:
            break

        if token.ttype is Keyword:
            if not token.value.upper().startswith(('INNER', 'LEFT', 'RIGHT', 'FULL')):
                break
        elif token.ttype is Name or isinstance(token, (Identifier, IdentifierList)):
            tables.add(token.get_real_name() if isinstance(token, IdentifierList) else token.value)
            break  # Assuming the table name immediately follows JOIN keywords

    return tables


def extract_fields(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    fields = []
    parsed = sqlparse.parse(sql_content)
    for statement in parsed:
        if statement.get_type() == DML and 'SELECT' in statement.tokens[0].value.upper():
            fields.extend(extract_fields_from_select_statement(statement))

    return fields


def extract_fields_from_select_statement(statement):
    fields = []
    tokens = iter(statement.tokens)
    in_select_clause = False
    for token in tokens:
        if token.ttype is Keyword and token.value.upper() == 'SELECT':
            in_select_clause = True
            continue

        if in_select_clause:
            if token.ttype is Punctuation and token.value == ',':
                continue
            elif token.ttype in (Keyword, Punctuation) and token.value.upper() in (
            'FROM', 'WHERE', 'GROUP BY', 'ORDER BY'):
                break
            elif token.ttype is Name or isinstance(token, (Identifier, IdentifierList)):
                fields.append(token.get_real_name() if isinstance(token, IdentifierList) else token.value)

    return fields


# Example usage
file_path = r"C:\Users\admin\Desktop\dwd_t04_reelshort_opc_push_detail_di.sql"
tables = extract_tables(file_path)
fields = extract_fields(file_path)
print("Tables:", tables)
print("Fields:", fields)