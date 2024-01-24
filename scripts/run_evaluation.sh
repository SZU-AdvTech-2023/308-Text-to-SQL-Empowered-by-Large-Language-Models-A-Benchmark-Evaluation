db_root_path='./dataset/bird/database/'
data_mode='dev'
predicted_sql_path='./output/'
ground_truth_path='./dataset/bird/dev/'
diff_json_path="./dataset/bird/dev/dev.json"
num_cpus=16
time_out=60
mode_gt='gt'
mode_predict='gpt'

python -u ./src/get_final_output.py  --root_path ./dataset/process/BIRD-TEST_OPENSQL_9-SHOT_EUCDISQUESTIONMASK_QA-EXAMPLE_CTX-200_ANS-4096
python -u ./src/evaluation.py \
    --db_root_path ${db_root_path} \
    --predicted_sql_path ${predicted_sql_path} \
    --data_mode ${data_mode} \
    --ground_truth_path ${ground_truth_path} \
    --num_cpus ${num_cpus} \
    --time_out ${time_out} \
    --mode_gt ${mode_gt} \
    --mode_predict ${mode_predict} \
    --diff_json_path ${diff_json_path}

