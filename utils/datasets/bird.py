import json
import re
import sqlite3
from copy import copy
from pathlib import Path
from typing import List, Dict
import pandas as pd
import os 
import attr
import torch
import networkx as nx
from tqdm import tqdm



def get_describe(dataset_path, db_name, table_name, col_name):
    root_path = os.path.join(dataset_path, "database")
        
    # print(db_name, table_name)
    table_csv_path = root_path + "/" + db_name + "/database_description" + "/" + table_name + ".csv"
    instruction = {}
    if not os.path.exists(table_csv_path):
        return instruction
    df = pd.read_csv(table_csv_path, encoding='unicode_escape')
    df.dropna()
    df.fillna('undefine')
    # print(table_csv_path)
    for index, row in df.iterrows():
        key1 = [key for key in row.keys() if 'column_name' in key]
        key2 = [key for key in row.keys() if 'column_description' in key]
        # key2 = [key for key in row.keys() if 'data_format' in key]
        # key2 = [key for key in row.keys() if 'column_name' in key]
        # print(key1[0])
        cname = str(row[key1[0]])
        # cname = row['column_name'] if pd.isna('column_name') else row[key1[0]]
        if type(row[key2[0]]) == str:
            describe = "(" + row[key2[0]].strip(" ") + ")"
        else:
            describe = ""
        
        instruction[cname] = describe
    col_describe_info = instruction.get(col_name, " ")
    if col_describe_info.strip('(').strip(')').lower() == col_name.lower():
        col_describe_info = " "
    return col_describe_info


def get_some_values(dataset_path, database_name, table_name, column_name, data_type):
    """
       {'', 'varchar(15)', 'blob', 'datetime', 'integer unsigned', 'real', 'date', 'varchar', 'float', 'text', 'int', 'integer'}
       {'', 'real', 'text', 'datetime', 'date', 'numeric', 'float', 'integer', 'varchar'}
       """
    # 连接数据库
    root_path = os.path.join(dataset_path, "database")

    if column_name == "*":
        return ""
    
    database_file  = root_path + "/" + database_name + "/" + database_name + ".sqlite"
    conn = sqlite3.connect(database_file)
    # 创建一个游标对象
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL")
    
    # 获取所有不同的取值
    distinct_values = cursor.fetchall()
    if len(distinct_values) == 0:
        return " "
    distinct_values = [value[0] if type(value) == tuple else value for value in distinct_values]
    if "int" in data_type:
        if len(distinct_values) >=2 and len(distinct_values) <= 3:
            values = [ str(x) for x in distinct_values]
            value_info = "(" + ", ".join(values) + ")"
            return value_info
    elif "text" in data_type:
        sorted_distinct_values = sorted(distinct_values, key=len, reverse=True)
        if len(distinct_values) >=2 and len(distinct_values) <= 5:
            if len(sorted_distinct_values[0]) <= 15:
                values = [ str(x) for x in distinct_values]
                value_info = "(" + ", ".join(values) + ")"
                return value_info
    return " "




def build_foreign_key_map(entry):
    cols_orig = entry["column_names_original"]
    tables_orig = entry["table_names_original"]

    # rebuild cols corresponding to idmap in Schema
    cols = []
    for col_orig in cols_orig:
        if col_orig[0] >= 0:
            t = tables_orig[col_orig[0]]
            c = col_orig[1]
            cols.append("__" + t.lower() + "." + c.lower() + "__")
        else:
            cols.append("__all__")

    def keyset_in_list(k1, k2, k_list):
        for k_set in k_list:
            if k1 in k_set or k2 in k_set:
                return k_set
        new_k_set = set()
        k_list.append(new_k_set)
        return new_k_set

    foreign_key_list = []
    foreign_keys = entry["foreign_keys"]
    for fkey in foreign_keys:
        key1, key2 = fkey
        if key1 and key2:
            key_set = keyset_in_list(key1, key2, foreign_key_list)
            key_set.add(key1)
            key_set.add(key2)
    foreign_key_map = {}
    for key_set in foreign_key_list:
        sorted_list = sorted(list(key_set))
        midx = sorted_list[0]
        for idx in sorted_list:
            foreign_key_map[cols[idx]] = cols[midx]

    return foreign_key_map



@attr.s
class BIRDItem:
    text = attr.ib()
    code = attr.ib()
    schema = attr.ib()
    orig = attr.ib()
    orig_schema = attr.ib()


@attr.s
class Column:
    id = attr.ib()
    table = attr.ib()
    name = attr.ib()
    unsplit_name = attr.ib()
    orig_name = attr.ib()
    type = attr.ib()
    foreign_key_for = attr.ib(default=None)
    


@attr.s
class Table:
    id = attr.ib()
    name = attr.ib()
    unsplit_name = attr.ib()
    orig_name = attr.ib()
    columns = attr.ib(factory=list)
    primary_keys = attr.ib(factory=list)


@attr.s
class Schema:
    db_id = attr.ib()
    tables = attr.ib()
    columns = attr.ib()
    foreign_key_graph = attr.ib()
    orig = attr.ib()
    connection = attr.ib(default=None)


def postprocess_original_name(s: str):
    return re.sub(r'([A-Z]+)', r' \1', s).replace('_', ' ').lower().strip()


def load_tables(paths):
    schemas = {}
    eval_foreign_key_maps = {}

    for path in paths:
        schema_dicts = json.load(open(path))
        dataset_path = "/".join(path.split("/")[:-1])
        
        for schema_dict in schema_dicts:
            db_id = schema_dict['db_id']

            tables = tuple(
                Table(
                    id=i,
                    name=name.split(),
                    unsplit_name=name,
                    orig_name=orig_name,
                )
                for i, (name, orig_name) in enumerate(zip(
                    schema_dict['table_names'], schema_dict['table_names_original']))
            )
            
            columns = tuple(
                Column(
                    id=i,
                    table=tables[table_id] if table_id >= 0 else None,
                    name=col_name.split(),
                    unsplit_name=col_name,
                    orig_name=orig_col_name,
                    type=col_type,
                )
                for i, ((table_id, col_name), (_, orig_col_name), col_type) in enumerate(zip(
                    schema_dict['column_names'],
                    schema_dict['column_names_original'],
                    schema_dict['column_types']))
            )
            # print(tables, columns)
            # Link columns to tables
            for column in columns:
                if column.table:
                    column.table.columns.append(column)

            for column_id in schema_dict['primary_keys']:
                # Register primary keys
                if type(column_id) == list:
                    for id in column_id:
                        if id < len(columns) :
                            column = columns[id]  
                            column.table.primary_keys.append(column)
                else:
                    if column_id < len(columns) :
                        column = columns[column_id]
                        column.table.primary_keys.append(column)

            foreign_key_graph = nx.DiGraph()
            # print("LENGTH COLUMNS": len(columns) )
            for source_column_id, dest_column_id in schema_dict['foreign_keys']:
                # Register foreign keys
                if source_column_id and dest_column_id:
                    if source_column_id < len(columns) and dest_column_id < len(columns):
                        source_column = columns[source_column_id] 
                        dest_column = columns[dest_column_id]
                        # print(source_column.orig_name,  dest_column.orig_name)
                        source_column.foreign_key_for = dest_column
                        foreign_key_graph.add_edge(
                            source_column.table.id,
                            dest_column.table.id,
                            columns=(source_column_id, dest_column_id))
                        foreign_key_graph.add_edge(
                            dest_column.table.id,
                            source_column.table.id,
                            columns=(dest_column_id, source_column_id))

            
            assert db_id not in schemas
            schemas[db_id] = Schema(db_id, tables, columns, foreign_key_graph, schema_dict)
            eval_foreign_key_maps[db_id] = build_foreign_key_map(schema_dict)

    return schemas, eval_foreign_key_maps

