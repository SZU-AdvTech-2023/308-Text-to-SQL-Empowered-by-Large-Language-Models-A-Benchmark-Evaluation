import os 
import json 
import argparse


parser = argparse.ArgumentParser()
parser.add_argument(
        "--root_path",
    )

args = parser.parse_args()
root_path = args.root_path

def process_duplication(sql):
    sql = sql.strip().split("/*")[0]
    return sql


if __name__ == "__main__":
    input_dir = root_path
    pridict_path = os.path.join(input_dir, "predict.txt")
    input_path = os.path.join(input_dir, "output.jsonl")
    f = open(input_path, "r", encoding="utf-8")
    f_write = open(pridict_path, "w", encoding="utf-8")
    input_data = f.readlines()

    for item in input_data:
        item = json.loads(item)
        for key , value in item.items():
            sql = " ".join(value.replace("\n", " ").split())
            sql = process_duplication(sql)
            if sql.startswith("SELECT"):
                pass
            elif sql.startswith(" "):
                sql = "SELECT" + sql
            else:
                sql = "SELECT " + sql
            f_write.write(sql + "\n")

    f_write.close()
    f.close()
        
