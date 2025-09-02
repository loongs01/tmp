import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Punctuation, Name
from sqlparse.tokens import Keyword, Punctuation, Name, Whitespace


def extract_tables(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()
        # 打印文件内容
        print(sql_content)

    tables = set()
    parsed = sqlparse.parse(sql_content)[0]  # Assuming only one statement for simplicity
    tokens = iter(parsed.tokens)

    while True:
        try:
            token = next(tokens)
        except StopIteration:
            break

        if token.ttype is Keyword:
            if token.value.upper() == 'FROM':
                # Skip any whitespace or punctuation after 'FROM'
                while True:
                    token = next(tokens)
                    if token.ttype not in (Whitespace, Punctuation):
                        break
                # Now 'token' should be the first part of the table name or alias
                tables.add(extract_table_or_alias(token, tokens))
            elif token.value.upper().startswith('JOIN'):
                # Skip the JOIN keyword and any following whitespace or punctuation
                while True:
                    token = next(tokens)
                    if token.ttype not in (Whitespace, Punctuation):
                        break
                # Now 'token' should be the first part of the joined table name or alias
                tables.add(extract_table_or_alias(token, tokens))

    return tables


def extract_table_or_alias(start_token, tokens):
    # This function extracts a table name or alias starting from 'start_token'
    # and continues until it encounters a keyword, DML statement, punctuation, or whitespace
    # that indicates the end of the name.
    parts = []
    while True:
        if isinstance(start_token, (Identifier, IdentifierList)):
            parts.append(start_token.get_real_name())  # For IdentifierList, this might need adjustment
        elif start_token.ttype is Name:  # In case the table name is not quoted
            parts.append(start_token.value)

        # Check for the end of the table/alias name
        try:
            next_token = next(tokens)
        except StopIteration:
            break

        if next_token.ttype in (Keyword, DML, Punctuation) or (next_token.ttype is Whitespace and not parts):
            # If we hit a keyword, DML, punctuation, or leading whitespace, we're done
            break

        # Otherwise, continue with the next token
        start_token = next_token

    # Join parts into a single string (this might not be necessary if IdentifierList is handled correctly)
    return '.'.join(parts) if parts else ''


# Note: Whitespace is imported from sqlparse.tokens, but it might not be necessary
# depending on how you want to handle whitespace around table names/aliases.

# Example usage (ensure this is outside the function definitions)
file_path = r"C:\Users\admin\Desktop\tmp\dwd_t04_reelshort_opc_push_detail_di.sql"
tables = extract_tables(file_path)
print("Tables:", tables)
