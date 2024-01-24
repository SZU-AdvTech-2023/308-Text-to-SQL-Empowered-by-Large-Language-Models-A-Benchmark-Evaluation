import pandas as pd 
import json 
import numpy as np 
from pathlib import Path
import os 


question_path = "/public24_data/qth/DAIL-SQL/dataset/process/BIRD-TEST_OPENSQL_9-SHOT_EUCDISQUESTIONMASK_QA-EXAMPLE_CTX-200_ANS-4096/questions.json"
question_json = json.load(open(question_path))
questions = question_json["questions"]
input_dir = "/public24_data/qth/DAIL-SQL/dataset/process/BIRD-TEST_OPENSQL_9-SHOT_EUCDISQUESTIONMASK_QA-EXAMPLE_CTX-200_ANS-4096"
input_path = os.path.join(input_dir, "input.jsonl")
f = open(input_path, "w")

for idx, item in enumerate(questions):
    question = item['prompt']
    input_data = {}
    input_data["index"] = idx
    input_data["instruction"]  = ""
    input_data["input"] = question
    f.write(json.dumps(input_data, ensure_ascii=False) + "\n")
f.close()
    