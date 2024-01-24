"""
分析sql错误原因
"""
import json
import os.path
import re
import sqlite3

import nltk
from nltk import word_tokenize

def tokenize(string):
    string = str(string)
    string = string.replace("\'", "\"")
    toks = [word for word in word_tokenize(string)]
    # find if there exists !=, >=, <=
    eq_idxs = [idx for idx, tok in enumerate(toks) if tok == "="]
    eq_idxs.reverse()
    prefix = ('!', '>', '<')
    for eq_idx in eq_idxs:
        pre_tok = toks[eq_idx-1]
        if pre_tok in prefix:
            toks = toks[:eq_idx-1] + [pre_tok + "="] + toks[eq_idx+1: ]
    # 如果形式为t1.name则需要组成一个单词
    # 定义正则表达式模式，匹配数字或 "t数字" 形式
    pattern = r"^\d$|^t\d$"
    merge_range = []
    dot_indexes = [index for index, tok in enumerate(toks) if tok == "." and toks[index + 1] == "`" and re.match(pattern, toks[index-1])]
    for dot_index in dot_indexes:
        start, end = find_range(toks, dot_index)
        merge_range.append((start, end))
        #toks = toks[:start] + [" ".join(toks[start:end])] + toks[end:]
    pattern = r"^t\d\.$"
    #pattern1 = r"^`[^`]*`"
    #dot_indexes = [idx for idx, tok in enumerate(toks) if re.match(pattern, toks) and (toks[idx + 1] == "`" or re.match(pattern1, toks[idx + 1]))]
    dot_indexes = [idx for idx, tok in enumerate(toks) if re.match(pattern, tok) and toks[idx + 1] == "`"]
    for dot_index in dot_indexes:
        end = find_backquote_end(toks, dot_index + 1)
        merge_range.append((dot_index, end + 1))
        #toks = toks[:dot_index] + [" ".join(toks[dot_index:end + 1])] + toks[end + 1:]
    if len(merge_range) == 0:
        return toks
    merge_toks = []
    last_end = 0
    for idx, (start, end) in enumerate(merge_range):
        if idx == 0:
            merge_toks.extend(toks[:start])
        else:
            merge_toks.extend(toks[last_end:start])
        merge_item = " ".join(toks[start:end])
        merge_item = re.sub(r'\s*\.\s*', '.', merge_item)
        merge_toks.append(merge_item)
        last_end = end
    merge_toks.extend(toks[last_end:])
    return merge_toks


def is_valid_SQL(db, sql):
    """
    执行sql语句
    :param db:
    :param sql:
    :return:
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except Exception as e:
        return e.__str__()
    return "正确"

def mul_sql_is_correct(p_sql: str, g_sql: str, db: str):
    """
    判断sql的多语句操作是否正确，即判断是否含有Except、Union和Intersect等关键字
    :param p_sql:
    :param g_sql:
    :return:
    """
    p_except = p_sql.count("except")
    p_union = p_sql.count("union")
    p_intersect = p_sql.count("intersect")
    g_except = g_sql.count("except")
    g_union = g_sql.count("union")
    g_intersect = g_sql.count("intersect")

    # 判断是否不使用多语句操作正确
    if p_except == g_except == 0 and p_union == g_union == 0 and p_intersect == g_intersect == 0:
        return 1, None

    if p_except == g_except  and p_union == g_union  and p_intersect == g_intersect:
        g_sub_sqls = g_sql.split("except")
        g_sub_sqls = [substring.split("union") for substring in g_sub_sqls]
        g_sub_sqls = [item.split("intersect") for sublist in g_sub_sqls for item in sublist]
        p_sub_sqls = p_sql.split("except")
        p_sub_sqls = [substring.split("union") for substring in p_sub_sqls]
        p_sub_sqls = [item.split("intersect") for sublist in p_sub_sqls for item in sublist]
        # TODO：多语句的顺序无关，可能不是一一对应的
        for idx, p_sub_sql, g_sub_sql in enumerate(zip(p_sub_sqls,g_sub_sqls)):
            info = analyza_reason(p_sub_sql, g_sub_sql, db)
            return 0, info
    else:
        return -1, "多语句操作不正确"


def analyza_reason(p_sql, g_sql, db)-> str:
    """
    分析错误原因
    :param p_sql:
    :param g_sql:
    :param db:
    :return:
    """
    p_toks = tokenize(p_sql)
    g_toks = tokenize(g_sql)
    # 分析子查询
    mark, info = sub_sql_is_correct(p_sql, g_sql, p_toks, g_toks, db)
    if mark != 0:
        return info
    # 分析表是否正确
    mark, info = table_is_correct(p_toks, g_toks)
    if mark != 1:
        return info
    # 分析联表操作
    mark, info = join_is_correct(p_sql, g_sql, p_toks, g_toks)
    if mark != 1:
        return info
    # 分析条件语句是否正确
    mark, info = where_is_correct(p_sql, g_sql, p_toks, g_toks)
    if mark != 1:
        return info
    # 分析分组操作是否正确
    mark, info = group_is_correct(p_sql, g_sql, p_toks, g_toks)
    if mark == -1:
        return info
    # 分析列是否正确
    mark, info = column_is_correct(p_sql, g_sql, p_toks, g_toks, db)
    if mark != 1:
        return info

    return "其他未知原因错误"


def sub_sql_is_correct(p_sql: str, g_sql: str, p_toks, g_toks ,db):
    """
    判断子查询是否正确
    :param p_sql:
    :param g_sql:
    :param db:
    :return:
    """
    # 判断select的个数是否一致
    p_select_num = p_sql.count("select")
    g_select_num = g_sql.count("select")
    if p_select_num != g_select_num:
        return -1, "子查询不正确"
    if p_select_num == g_select_num == 1:
        return 0, None
    p_sub_sql = get_sub_sql(p_toks)
    g_sub_sql = get_sub_sql(g_toks)
    info = analyza_reason(p_sub_sql, g_sub_sql, db)
    if info == "其他未知原因错误":
        return 0, None
    return 1, "子查询中的：" + info


def join_is_correct(p_sql: str, g_sql: str, p_toks, g_toks):
    """
    判断联表操作是否正确
    如果join的个数不一致，则不正确，如果on后面的表名不一致，则不正确
    :param p_sql:
    :param g_sql:
    :return: -1 表示join操作不正确；0表示表不正确；1表示join操作正确
    """
    p_join_num = p_sql.count("join")
    g_join_num = g_sql.count("join")
    if p_join_num != g_join_num:
        return -1, "联表操作不正确"
    elif p_join_num == g_join_num == 0:
        return 1, None
    # 对比表是否正确，如果是全连接或内连接，则from之后的表和on之后的表只要一致即可，不需要关注顺序。在set集合中对比
    # 如果是左连接或右连接，则还需要顺序一致。在字典中对比


    p_on_indexes = find_all_indices(p_toks, "on")
    p_on_list = []
    print(p_sql)
    print(p_toks)
    for index in p_on_indexes:
        if p_toks[index + 2] != "=":
            continue
        assert p_toks[index + 2] == "="
        p_table1 = p_toks[index + 1]
        if "." in p_table1:
            alias, column = p_table1.split(".")
            print(alias, column)
            p_alias_index = find_alias_index(p_toks, alias)
            assert p_alias_index > 0
            ture_name = p_toks[p_alias_index - 1]
            p_table1 = ture_name + "." + column
        p_table2 = p_toks[index + 3]
        if "." in p_table2:
            alias, column = p_table2.split(".")
            p_alias_index = find_alias_index(p_toks, alias)
            assert p_alias_index > 0
            ture_name = p_toks[p_alias_index - 1]
            p_table2 = ture_name + "." + column
        p_on_list.append({p_table1, p_table2})
    g_on_indexes = find_all_indices(g_toks, "on")
    g_on_list = []
    print(p_sql)
    print(p_toks)
    for index in g_on_indexes:
        if g_toks[index + 2] != "=":
            continue
        assert g_toks[index + 2] == "="
        g_table1 = g_toks[index + 1]
        if "." in g_table1:
            alias, column = g_table1.split(".")
            g_alias_index = find_alias_index(g_toks, alias)
            assert g_alias_index > 0
            ture_name = g_toks[g_alias_index - 1]
            g_table1 = ture_name + "." + column
        g_table2 = g_toks[index + 3]
        if "." in g_table2:
            alias, column = g_table2.split(".")
            g_alias_index = find_alias_index(g_toks, alias)
            assert g_alias_index > 0
            ture_name = g_toks[g_alias_index - 1]
            g_table2 = ture_name + "." + column
        g_on_list.append({g_table1, g_table2})
    if p_on_list != g_on_list:
        return -1 ,"联表操作不正确"
    return 1, None


def group_is_correct(p_sql: str, g_sql: str, p_toks, g_toks):
    """
    判断分组操作是否正确
    如果group个数不一致，则不正确；如果group之后的列名不一致，则不正确
    :param p_sql:
    :param g_sql:
    :return:
    """
    p_group_num = p_sql.count("group")
    g_group_num = g_sql.count("group")
    if p_group_num != g_group_num:
        return -1, "分组操作不正确"
    elif p_group_num == g_group_num == 0:
        return 0, None
    p_group_index = p_toks.index("group")
    p_group_column = p_toks[p_group_index + 2]
    if "." in p_group_column:
        p_group_column = p_group_column.split(".")[1]
    g_group_index = g_toks.index("group")
    g_group_column = g_toks[g_group_index + 1]
    if "." in g_group_column:
        g_group_column = g_group_column.split(".")[1]
    if p_group_column == g_group_column:
        return 1, None
    else:
        return -1, "分组操作不正确"



def table_is_correct(p_toks, g_toks):
    """
    判断表是否正确
    比较第一个from之后的表是否一致
    :param p_sql:
    :param g_sql:
    :return: -1 表示没有from，即语法错误；0表示表不正确；1表示表正确
    """
    p_join_indexes = find_all_indices(p_toks,"join")
    g_join_indexes = find_all_indices(g_toks,"join")
    # 如果join关键字个数不一样则判定为联表不正确
    if len(p_join_indexes) != len(g_join_indexes):
        return 0, "联表不正确"
    p_tables = []
    g_tables = []
    p_from_index = p_toks.index("from")
    p_tables.append({p_toks[p_from_index + 1]})
    g_from_index = g_toks.index("from")
    g_tables.append({g_toks[g_from_index + 1]})
    if len(p_join_indexes) == 0:
        if p_tables != g_tables:
            return -1, "表不正确"
        else:
            return 1, None
    for p_join_index in p_join_indexes:
        if p_toks[p_join_index - 1] != "left" or p_toks[p_join_index - 1] != "right":
            if type(p_tables[len(p_tables) - 1]) == set:
                p_tables[len(p_tables) - 1].add(p_toks[p_join_index + 1])
            else:
                p_tables.append({p_toks[p_join_index + 1]})
        elif p_toks[p_join_index - 1] == "right":
            p_tables.append(p_toks[p_join_index + 1])
            p_tables[len(p_tables)-1], p_tables[len(p_tables)-2] = p_tables[len(p_tables)-2], p_tables[len(p_tables)-1]
        else:
            p_tables.append(p_toks[p_join_index + 1])
    for g_join_index in g_join_indexes:
        if g_toks[g_join_index - 1] != "left" or g_toks[g_join_index - 1] != "right":
            if type(g_tables[len(g_tables) - 1]) == set:
                g_tables[len(g_tables) - 1].add(g_toks[g_join_index + 1])
            else:
                g_tables.append({g_toks[g_join_index + 1]})
        elif g_toks[g_join_index - 1] == "right":
            g_tables.append(g_toks[g_join_index + 1])
            g_tables[len(g_tables)-1], g_tables[len(g_tables)-2] = g_tables[len(g_tables)-2], g_tables[len(g_tables)-1]
        else:
            g_tables.append(g_toks[g_join_index + 1])
    if p_tables[0] == g_tables[0]:
        return 1, None
    else:
        if type(p_tables[0]) == set and type(g_tables[0]) == set and p_toks[p_from_index + 1] == g_toks[g_from_index + 1]:
            return 0, "联表不正确"
        else:
            return -1, "表不正确"



def column_is_correct(p_sql: str, g_sql: str, p_toks, g_toks, db: str) :
    """
    判断列名是否正确
    通过执行sql语句，获取结果中所有列的名称，比较是否一致
    :param p_sql:
    :param g_sql:
    :param db:
    :return:
    """
    pattern = r't\d+\.'
    p_select_index = p_toks.index("select")
    p_from_index = p_toks.index("from")
    p_columns_list = " ".join(p_toks[p_select_index + 1: p_from_index]).split(",")
    p_columns = set()
    for p_column in p_columns_list:
        p_columns.add(re.sub(pattern, '', p_column))
    g_select_index = g_toks.index("select")
    g_from_index = g_toks.index("from")
    g_columns_list = " ".join(g_toks[g_select_index + 1: g_from_index]).split(",")
    g_columns = set()
    for g_column in g_columns_list:
        g_columns.add(re.sub(pattern, '', g_column))
    if g_columns == p_columns:
        return 1, None
    else:
        return -1, "列不正确"



def where_is_correct(p_sql, g_sql, p_toks, g_toks):
    """
    判断条件语句是否正确
    :param p_sql:
    :param g_sql:
    :param p_toks:
    :param g_toks:
    :return:
    """
    # 提取条件语句
    try:
        p_where_start = p_toks.index("where")
    except ValueError:
        if "where" in g_sql:
            return -1, "条件语句不正确"
        else:
            return 1, None
    try:
        g_where_start = g_toks.index("where")
    except ValueError:
        return -1, "条件语句不正确"
    p_where_dict = dict()
    g_where_dict = dict()

    p_where_end = find_where_end(p_toks, p_where_start)
    p_or_list = " ".join(p_toks[p_where_start + 1: p_where_end]).split("or")
    g_where_end = find_where_end(g_toks, g_where_start)
    g_or_list = " ".join(g_toks[g_where_start + 1: g_where_end]).split("or")
    if len(p_or_list) != len(g_or_list):
        return -1, "条件语句不正确"

    p_where_list = []
    for where in p_or_list:
        p_where_list.extend(where.split("and"))
    pattern = r"t\d\."
    for idx, where in enumerate(p_where_list):
        aliases = re.findall(pattern, where)
        for alias in aliases:
            alias = alias.rstrip(".")
            p_alias_index = find_alias_index(p_toks, alias)
            assert p_alias_index > 0
            ture_name = p_toks[p_alias_index - 1] + "."
            p_where_list[idx]  = where= re.sub(pattern, ture_name, where)
        # if "=" in where:
        #     equals_where = where.split("=")
        #     p_where_list[idx] = set(equals_where)
        if idx == 0:
            continue
        if "between" in p_where_list[idx - 1]:
            p_where_list[idx - 1] = p_where_list[idx - 1] + " " + where
            p_where_list[idx] = ""
    p_where_set = {(item,) for item in p_where_list if item}

    g_where_list = []
    for where in g_or_list:
        g_where_list.extend(where.split("and"))
    for idx, where in enumerate(g_where_list):
        aliases = re.findall(pattern, where)
        for alias in aliases:
            alias = alias.rstrip(".")
            g_alias_index = find_alias_index(g_toks, alias)
            assert g_alias_index > 0
            ture_name = g_toks[g_alias_index - 1] + "."
            g_where_list[idx]  = where = re.sub(pattern, ture_name, where)
        # if "=" in where:
        #     equals_where = where.split("=")
        #     g_where_list[idx] = set(equals_where)
        if idx == 0:
            continue
        if "between" in g_where_list[idx - 1]:
            g_where_list[idx - 1] = g_where_list[idx - 1] + " " + where
            g_where_list[idx] = ""
    g_where_set = {item for item in g_where_list if item}
    if p_where_set == g_where_set:
        return 1, None
    else:
        return -1, "条件语句不正确"

def get_sub_sql(toks):
    """
    提取子查询对应的sql语句
    """
    # 查询sql语句中第二个select对应的索引

    end_index = len(toks) - 1
    start_index = toks.index("select", 1)
    stack = ["("]
    for idx, char in enumerate(toks[start_index:]):
        if char == "(":
            stack.append("(")
        if char == ")":
            stack.pop()
            if len(stack) == 0:
                end_index = idx + start_index
                break
    return " ".join(toks[start_index: end_index])


def find_all_indices(text, char_to_find):
    indices = []
    # 使用循环查找字符的所有索引位置
    for i, char in enumerate(text):
        if char == char_to_find:
            indices.append(i)
    return indices


def rfind(toks, index, char_to_find):
    for idx in range(index - 1, 0, -1):
        if toks[idx] == char_to_find:
            return idx
    return -1


def find_where_end(toks, index):
    for idx, tok in enumerate(toks[index + 1:]):
        if tok == "order" or (tok == "group" and toks[idx + 1] == "by"):
            return idx + index + 1
    return len(toks)


def find_alias_index(toks, alias_name):
    indexes = find_all_indices(toks, "as")
    for index in indexes:
        if toks[index + 1] == alias_name:
            return index
    for idx, tok in enumerate(toks):
        if tok == alias_name:
            return idx
    return -1


def find_backquote_end(toks, index):
    for idx, tok in enumerate(toks[index+1:]):
        if tok == "`":
            return idx + index + 1
    return -1

def find_range(toks, dot_index):
    pattern1 = r"^\d$"
    pattern2 = r"^t\d$"
    backquote_start = dot_index + 1
    assert toks[dot_index + 1] == "`"
    backquote_end = find_backquote_end(toks, backquote_start)
    assert backquote_end > 0
    if re.match(pattern1, toks[dot_index - 1]):
        assert toks[dot_index - 2] == "t"
        return dot_index - 2, backquote_end + 1
    if re.match(pattern2, toks[dot_index - 1]):
        return dot_index - 1, backquote_end + 1

if __name__ == "__main__":
    # folder = "BIRD-TEST_OPENSQL_9-SHOT_EUCDISQUESTIONMASK_QA-EXAMPLE_CTX-200_ANS-4096"
    input_path = "./errors/OPENSQL_res.json"
    save_path = "./errors/OPENSQL_error.json"
    dev_data = json.load(open(input_path, "r"))
    #dev_data = json.load(open("../../result_data/data/error.json", "r"))
    db_dir = "./dataset/bird/database"
    for idx, data in enumerate(dev_data):
        if data["result"] == 1:
            print(idx, "正確")
            continue
        if data["sql_output"] == "":
            continue
        # if idx==35:
        #      print("----------")
        p_sql = data["sql_output"]
        g_sql = data["golden_sql"]
        db_name = data["db_id"]
        db = os.path.join(db_dir, db_name, db_name + ".sqlite")
        info = is_valid_SQL(db, p_sql)
        if "no such table" in info:
            data["reason"] = "表不存在"
            continue
        if "no such column" in info:
            data["reason"] = "列不存在"
            continue
        if info != "正确":
            data["reason"] = "无效sql语句:" + info
            continue
        p_sql = p_sql.lower()
        g_sql = g_sql.lower()

        # 多语句操作
        mark, info = mul_sql_is_correct(p_sql, g_sql, db)
        if mark != 1:
            data["reason"] = info
            continue
        info = analyza_reason(p_sql, g_sql, db)
        data["reason"] = info
        print("{}:{}".format(idx, info))
    with open(save_path, "w") as f:
        f.write(json.dumps(dev_data, ensure_ascii=False) + "\n")