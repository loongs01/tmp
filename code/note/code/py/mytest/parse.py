import re


def parse_sql_file(file_path):
    tables = set()
    columns = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    # Regular expression to find table names in SELECT, FROM, INSERT INTO, UPDATE, and DELETE statements
    table_pattern = re.compile(r'\b(?:FROM|JOIN|INTO|UPDATE|DELETE|TABLE)\s+([\w.]+)\b', re.IGNORECASE)
    column_pattern = re.compile(r'\bSELECT\s+(.*?)(?:FROM|$)', re.IGNORECASE | re.DOTALL)

    # Find all table names
    for match in table_pattern.finditer(sql_content):
        table = match.group(1).replace('`', '').replace('"', '').strip()
        tables.add(table.split('.')[-1])  # Extract the last part of the table name in case of schema.table

    # Find all column names
    for match in column_pattern.finditer(sql_content):
        columns_raw = match.group(1).replace('`', '').replace('"', '').strip()
        columns_list = re.split(r',\s*', columns_raw)

        for col in columns_list:
            if ' AS ' in col:
                col = col.split(' AS ')[0].strip()
            if '.' in col:
                col = col.split('.')[-1].strip()  # Extract the last part of the column name in case of table.column
            if col:
                # Add the column to the set of columns for each table (assuming simple context where table is implied by the nearest SELECT statement)
                # For more complex SQL, additional parsing logic would be needed to accurately associate columns with tables
                # Here, we'll just store columns in a flat list with a note about which table they are likely from (simplified)
                # If needed, a more sophisticated approach could be implemented to track context and accurately map columns to tables
                # For simplicity, we'll assume all columns belong to the last parsed table (a common but not always accurate assumption)
                if tables:
                    last_table = tables.pop()  # Assume the last parsed table is the one being referenced
                    if last_table not in columns:
                        columns[last_table] = set()
                    columns[last_table].add(col)

    # Note: The above logic for associating columns with tables is simplified.
    # In practice, parsing SQL to accurately associate columns with tables
    # requires more sophisticated context tracking, especially for complex queries.

    return tables, columns


# Example usage
file_path = r"C:\Users\admin\Desktop\dwd_t04_reelshort_opc_push_detail_di.sql"
tables, columns = parse_sql_file(file_path)

print("Tables:")
for table in tables:
    print(table)

print("\nColumns:")
for table, col_list in columns.items():
    print(f"{table}: {', '.join(col_list)}")
