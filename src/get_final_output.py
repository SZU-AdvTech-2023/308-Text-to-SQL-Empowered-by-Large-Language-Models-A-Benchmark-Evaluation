import os 
import numpy as np
import json 
import argparse


parser = argparse.ArgumentParser()
parser.add_argument(
        "--root_path",
    )

args = parser.parse_args()
root_path = args.root_path

def merge_output():
    output_dir = root_path 
    save_path = os.path.join(output_dir , "output_convert.jsonl")
    save_file = open(save_path , 'w' , encoding = 'utf-8')
    refer_evaluation_bird_path = "./dataset/bird/result_codellama7b_0909.json"  
    #get all data from splited file
    split_files_path = os.path.join(output_dir , "output.jsonl")
    #get total split data 
    pred_data = {}
    with open(split_files_path,'r' ) as f:
        split_data = f.readlines()
        for line in split_data:
            item = json.loads(line)
            for key , value in item.items():
                if value :
                    if  str(value).startswith("SELECT"):
                        pred_data[int(key)] = str(value).replace("\n", " ")
                    else:
                        pred_data[int(key)] = "SELECT " + str(value).replace("\n", " ")
                else:
                    pred_data[int(key)] = "SELECT "
    # print(pred_data.keys())
    with open(refer_evaluation_bird_path, 'r') as f :
        pred_list = f.readlines()
        for item in pred_list:
            item = json.loads(item)
            id = item["cov_id"]
            if id in pred_data.keys():
                # print(keys)
                item["sql_output"] = pred_data[id]
            else:
                item["sql_output"] = None
            save_file.write(json.dumps(item, ensure_ascii=False) + "\n")
    save_file.close()

if __name__ == "__main__":  
    
    merge_output()
    out_path = root_path + "/output_convert.jsonl"
    final_output_path = "./output/predict_dev.json"
    final_output_sql = root_path + "/output_sql.txt"
    f_sql = open(final_output_sql, 'w', encoding = 'utf-8')
    final_data = {}
    with open(out_path, 'r' ,encoding = 'utf-8') as f :
        out_data = f.readlines()
        for item in out_data:
            item = json.loads(item)
            sql = item['sql_output'].replace("|| \', \' ||", "," )
            sql = sql.replace("|| \' \' ||", ",")
            sql = sql.replace("[", "")
            sql = sql.replace("]", "")
            for i in range(5):
                sql = sql.replace("  ", " ")
            # print(sql)
            final_data[item["cov_id"]] = sql + "\t----- bird -----\t" + item["db_id"]
            f_sql.write(sql + "\n")
        f.close()
        f_sql.close()
    with open(final_output_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(final_data, ensure_ascii=False))
        f.close()
        