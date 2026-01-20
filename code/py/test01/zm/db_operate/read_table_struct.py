import sys
import pymysql
from pymysql.err import MySQLError
from typing import Dict, Optional, List, Tuple

# StarRocks 连接配置
STARROCKS_CONFIG = {
    "host": "10.2.8.36",
    "port": 9030,
    "user": "root",
    "password": "iPXE83EEZSrOBUfe",
    "database": "test",
    "charset": "utf8",
    # pymysql expects int port
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_substitution_data():
    """从 StarRocks 获取替代物料数据（使用 PyMySQL，纯 Python 实现，避免 C 扩展崩溃）"""
    connection = None
    cursor = None
    try:
        connection = pymysql.connect(
            host=STARROCKS_CONFIG["host"],
            port=int(STARROCKS_CONFIG["port"]),
            user=STARROCKS_CONFIG["user"],
            password=STARROCKS_CONFIG["password"],
            database=STARROCKS_CONFIG["database"],
            charset=STARROCKS_CONFIG["charset"],
            cursorclass=STARROCKS_CONFIG["cursorclass"],
            autocommit=True,
            connect_timeout=10,
        )

        cursor = connection.cursor()

        query = """
        SELECT DISTINCT 
            matnr,
            idnrk_s,
            idnrk,
            alprf
        FROM ods.ods_sap_erp_substitutmaterials_test
        ORDER BY matnr, idnrk_s, alprf
        """

        cursor.execute(query)
        data = cursor.fetchall()
        print(data)
        return data

    except MySQLError as e:
        print(f"数据库连接或查询错误: {e}")
        return []
    except Exception as e:
        print(f"其他错误: {e}")
        return []
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if connection:
                connection.close()
        except Exception:
            pass


def _parse_table_identifier(identifier: str, default_db: str) -> Tuple[str, str]:
    """解析输入的表标识符，支持 `db.table` 或 `table`，以及反引号包裹。"""
    ident = identifier.strip()
    # 去掉两边反引号
    if ident.startswith("`") and ident.endswith("`") and ident.count("`") >= 2:
        ident = ident.strip("`")
    # 如果包含点，按库.表分割
    if "." in ident:
        parts = ident.split(".", 1)
        db = parts[0].strip("`")
        tbl = parts[1].strip("`")
        return db, tbl
    else:
        return default_db, ident.strip("`")


def get_create_statements(database: Optional[str] = None, include_views: bool = True) -> Dict[str, str]:
    """
    获取指定数据库中所有表/视图的建表语句（DDL）。
    :param database: 要导出的库名；None 则使用 STARROCKS_CONFIG 中的 database
    :param include_views: 是否包含视图
    :return: {对象名: DDL} 的字典，key 为表名（不含库名）
    """
    connection = None
    cursor = None
    ddls: Dict[str, str] = {}

    db = database or STARROCKS_CONFIG["database"]

    try:
        connection = pymysql.connect(
            host=STARROCKS_CONFIG["host"],
            port=int(STARROCKS_CONFIG["port"]),
            user=STARROCKS_CONFIG["user"],
            password=STARROCKS_CONFIG["password"],
            database=db,
            charset=STARROCKS_CONFIG["charset"],
            cursorclass=STARROCKS_CONFIG["cursorclass"],
            autocommit=True,
            connect_timeout=10,
        )
        cursor = connection.cursor()

        obj_types = ["BASE TABLE"]
        if include_views:
            obj_types.append("VIEW")

        placeholders = ", ".join(["%s"] * len(obj_types))
        list_sql = f"""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE IN ({placeholders})
            ORDER BY TABLE_NAME
        """
        cursor.execute(list_sql, (db, *obj_types))
        rows = cursor.fetchall()

        for row in rows:
            name = row["TABLE_NAME"]
            ttype = row["TABLE_TYPE"]

            if ttype == "BASE TABLE":
                sql = f"SHOW CREATE TABLE `{db}`.`{name}`"
            elif ttype == "VIEW":
                sql = f"SHOW CREATE VIEW `{db}`.`{name}`"
            else:
                continue

            cursor.execute(sql)
            res = cursor.fetchone()
            if not res:
                continue

            create_stmt = None
            for k, v in res.items():
                if k.lower().startswith("create"):
                    create_stmt = v
                    break
            if create_stmt is None:
                create_stmt = next(iter(res.values()))

            ddls[name] = create_stmt

        return ddls

    except MySQLError as e:
        print(f"数据库连接或查询错误: {e}")
        return {}
    except Exception as e:
        print(f"其他错误: {e}")
        return {}
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if connection:
                connection.close()
        except Exception:
            pass


def get_create_statements_for_tables(
    table_names: List[str],
    default_db: Optional[str] = None,
    include_views: bool = True,
) -> Dict[str, str]:
    """
    获取指定表组合的建表语句（支持跨库）。
    :param table_names: 表名列表，支持 "db.table" 或 "table"（使用 default_db）
    :param default_db: 未显式指定库时使用的默认库；None 则取 STARROCKS_CONFIG["database"]
    :param include_views: 是否包含视图（若指定对象为视图且该参数为 False，则跳过）
    :return: {库.表: DDL} 的字典，key 为 "db.table"
    """
    if not table_names:
        return {}

    connection = None
    cursor = None
    ddls: Dict[str, str] = {}
    db_fallback = default_db or STARROCKS_CONFIG["database"]

    try:
        connection = pymysql.connect(
            host=STARROCKS_CONFIG["host"],
            port=int(STARROCKS_CONFIG["port"]),
            user=STARROCKS_CONFIG["user"],
            password=STARROCKS_CONFIG["password"],
            database=db_fallback,  # 连接到默认库，执行时使用全限定名
            charset=STARROCKS_CONFIG["charset"],
            cursorclass=STARROCKS_CONFIG["cursorclass"],
            autocommit=True,
            connect_timeout=10,
        )
        cursor = connection.cursor()

        for raw_name in table_names:
            db, name = _parse_table_identifier(raw_name, db_fallback)

            cursor.execute(
                """
                SELECT TABLE_TYPE
                FROM information_schema.tables
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (db, name),
            )
            meta = cursor.fetchone()
            if not meta:
                print(f"未找到对象: {db}.{name}")
                continue

            ttype = meta["TABLE_TYPE"]
            if ttype == "BASE TABLE":
                show_sql = f"SHOW CREATE TABLE `{db}`.`{name}`"
            elif ttype == "VIEW":
                if not include_views:
                    continue
                show_sql = f"SHOW CREATE VIEW `{db}`.`{name}`"
            else:
                print(f"不支持的对象类型 {ttype}: {db}.{name}")
                continue

            cursor.execute(show_sql)
            res = cursor.fetchone()
            if not res:
                print(f"无法获取 DDL: {db}.{name}")
                continue

            create_stmt = None
            for k, v in res.items():
                if k.lower().startswith("create"):
                    create_stmt = v
                    break
            if create_stmt is None:
                create_stmt = next(iter(res.values()))

            ddls[f"{db}.{name}"] = create_stmt

        return ddls

    except MySQLError as e:
        print(f"数据库连接或查询错误: {e}")
        return {}
    except Exception as e:
        print(f"其他错误: {e}")
        return {}
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if connection:
                connection.close()
        except Exception:
            pass


def _read_table_names_from_file(file_path: str) -> List[str]:
    """
    从文本文件读取表名列表，支持：
      - 每行一个表名，或逗号分隔多个表名
      - 忽略空行和注释（以 # 或 -- 开头，或行内注释）
      - 支持 db.table 或仅 table
    """
    tables: List[str] = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                for sep in ["#", "--"]:
                    pos = raw.find(sep)
                    if pos == 0:
                        raw = ""
                        break
                    if pos > 0 and (sep == "#" or raw[pos - 1].isspace()):
                        raw = raw[:pos].strip()
                if not raw:
                    continue
                parts = [p.strip() for p in raw.split(",") if p.strip()]
                tables.extend(parts)
    except FileNotFoundError:
        print(f"文件不存在: {file_path}")
    except Exception as e:
        print(f"读取文件出错 {file_path}: {e}")
    return tables


def _unique_preserve_order(items: List[str]) -> List[str]:
    """去重并保持原顺序"""
    seen = set()
    result: List[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            result.append(it)
    return result


def _parse_cli_args(args: List[str]) -> Tuple[Optional[str], bool, List[str], Optional[str]]:
    """
    解析命令行参数：
    支持：
      - ddl [db_name] [--no-views] [--tables t1,t2,...] [t3 t4 ...] [--file path]
      - 若提供 --file，从文件读取表名；可与 --tables/位置参数合并，自动去重
    返回: (db_name, include_views, table_names, file_path)
    """
    db_name: Optional[str] = None
    include_views = True
    table_names: List[str] = []
    file_path: Optional[str] = None

    i = 0
    while i < len(args):
        token = args[i]
        if token == "--no-views":
            include_views = False
            i += 1
        elif token == "--tables":
            if i + 1 < len(args):
                table_names.extend([t for t in args[i + 1].split(",") if t.strip()])
                i += 2
            else:
                i += 1
        elif token in ("--file", "-f"):
            if i + 1 < len(args):
                file_path = args[i + 1]
                i += 2
            else:
                i += 1
        elif token == "--db":
            if i + 1 < len(args):
                db_name = args[i + 1]
                i += 2
            else:
                i += 1
        else:
            if db_name is None and not token.startswith("--"):
                db_name = token
            else:
                table_names.append(token)
            i += 1

    return db_name, include_views, table_names, file_path


if __name__ == "__main__":
    # 使用方式：
    # 1) 无参数：执行原替代物料数据查询
    # 2) ddl [db_name] [--no-views]：打印指定库（默认用配置中的 database）的所有对象建表语句
    # 3) ddl [db_name] [--no-views] --tables t1,t2 或追加位置参数 t3 t4：只导出指定表组合（支持 db.table）
    # 4) ddl [db_name] [--no-views] --file tables.txt：从文本文件读取表名（每行一个或逗号分隔；支持注释）
    #    可与 --tables/位置参数合并，自动去重；文件内可包含不同库的表如：ods.tbl_a,test.tbl_b
    if len(sys.argv) >= 2 and sys.argv[1].lower() == "ddl":
        cli_db, include_views, cli_tables, file_path = _parse_cli_args(sys.argv[2:])
        file_tables: List[str] = _read_table_names_from_file(file_path) if file_path else []
        all_tables = _unique_preserve_order([*cli_tables, *file_tables])

        if all_tables:
            ddls = get_create_statements_for_tables(
                table_names=all_tables,
                default_db=cli_db or STARROCKS_CONFIG["database"],
                include_views=include_views,
            )
            for fq_name, ddl in ddls.items():
                print(f"-- {fq_name}")
                print(ddl)
                print()
        else:
            ddls = get_create_statements(database=cli_db, include_views=include_views)
            for name, ddl in ddls.items():
                print(f"-- {name}")
                print(ddl)
                print()
    else:
        print(get_substitution_data())