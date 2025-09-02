# 初始化包含重复SQL文件名的列表
import sys

sql_file_list = [
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dws_data.dws_t85_reelshort_srv_currency_change_stat_di',
    'dws_data.dws_t85_reelshort_srv_currency_change_stat_di',
    'dwd_data.dwd_t04_reelshort_checkpoint_unlock_di',
    'dwd_data.dwd_t04_reelshort_checkpoint_unlock_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dwd_data.dwd_t04_reelshort_checkpoint_unlock_di',
    'dwd_data.dwd_t04_reelshort_checkpoint_unlock_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dws_data.dws_t85_reelshort_srv_currency_change_stat_di',
    'dws_data.dws_t85_reelshort_srv_currency_change_stat_di',
    'dwd_data.dwd_t05_reelshort_order_detail_di',
    'dwd_data.dwd_t05_reelshort_transaction_detail_di',
    'dwd_data.dwd_t05_reelshort_transaction_detail_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dwd_data.dwd_t02_reelshort_play_event_di',
    'dwd_data.dwd_t02_reelshort_play_event_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di',
    'dwd_data.dwd_t02_reelshort_custom_event_di'
]
# 使用set进行去重，但注意set不保证顺序
unique_sql_files_set = set(sql_file_list)
# 如果你需要保持原始顺序，可以使用以下方法
seen = set()
unique_sql_files_list = []
for item in sql_file_list:
    if item not in seen:
        unique_sql_files_list.append(item)
        seen.add(item)

# 打印去重后的列表
print(unique_sql_files_list)

if len(unique_sql_files_list) > 0:
    analysis_date = '2025-01-01'
    argsList = [["/home/etl/scripts/reelshort/dwd/dwd_t04_reelshort_opc_detail_di.sql", analysis_date],
                ["/home/etl/scripts/reelshort/dwd/dwd_t04_reelshort_opc_push_detail_di.sql", analysis_date]]
    for args in argsList:
        print(["python3", "/home/etl/scripts/reelshort/common/py_excute_sql.py"] + args)

downstream_tables = []
print(len(downstream_tables) - 1)

for i in range(0, len(downstream_tables) - 1):
    print(i)
data_table_relations = {}
data_table_relations['a'] = 1
print(data_table_relations)
print(data_table_relations.get('b', 'test'))

relations = {'a': ["/home/etl/scripts/reelshort/dwd/dwd_t04_reelshort_opc_detail_di.sql", analysis_date], 'b': 'test'}
# def find_downstream_tables_iterative(table_name, relations):
downstream_tables = [["python3", "/home/etl/scripts/reelshort/common/py_excute_sql.py"],'w']
stack = ['a','f']
print(stack)
dept = 0

while stack:
    dept += 1
    current_table = stack.pop()
    print(current_table)
    direct_downstream = relations.get(current_table, [])
    for downstream_table in direct_downstream:
        flag = 0
        # 删除重复元素
        for i in range(0, len(downstream_tables)):
            if downstream_tables[i][0] == current_table and downstream_tables[i][0] == downstream_table:
                flag = 1
                print('-----')
                print(i)
                mytest='循环'
                # downstream_tables.pop(i)
                print(flag)
        if flag == 0:
            print('dpt:'+str(dept))
            downstream_tables.append([dept, current_table, downstream_table])

        stack.append(downstream_table)
print('-----------')
print(downstream_tables)

print(stack)
print(i)
print(flag)



# test
from common.handleDB import HandleMysql

# handleMysql = HandleMysql(flag="adbUS3")

#
# def run(table_name):
#     sql = """
#         select from_table,GROUP_CONCAT(insert_table) as table_list
#         from check_data.metadata_table_relation
#         where from_table<>insert_table
#         group by from_table
#         """
#
#     datas = handleMysql.search(sql)
#     data_table_relations = {}
#     for data in datas:
#         data_table_relations[data[0]] = data[1].split(',')
#
#     # 递归获得表的所有下游
#     datas2 = find_downstream_tables_iterative(table_name, data_table_relations)
#     # datas2 = find_downstream_tables(table_name, data_table_relations)
#     list2 = []
#     for arr in datas2:
#         print(arr)
#         list2.append([table_name, arr[0], arr[1], arr[2]])

# 存储表血缘关系
# store_relations(table_name, list2)

# #展示血缘关系图
# view_graph(datas2)

# if __name__ == '__main__':
#     # table_name = sys.argv[1]
#     table_name = 'dwd_data.dwd_t02_reelshort_play_event_di'
#     print("解析表%s血缘关系..." % table_name)
#     # table_name = "dwd_data.dwd_t02_reelshort_item_pv_di"
#     # run(table_name)
#
# sql = """
#         select from_table,GROUP_CONCAT(insert_table) as table_list
#         from check_data.metadata_table_relation
#         where from_table<>insert_table
#         group by from_table
#         """
#
# datas = handleMysql.search(sql)
# data_table_relations = {}
# for data in datas:
#     data_table_relations[data[0]] = data[1].split(',')
#
# # 递归获得表的所有下游
# # datas2 = find_downstream_tables_iterative(table_name, data_table_relations)
#
# print(data_table_relations)
