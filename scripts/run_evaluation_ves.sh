db_root_path='./data/dev/dev_databases/'
data_mode='dev'
predicted_sql_path='./output/'
ground_truth_path='./data/dev/'
diff_json_path="./data/dev/dev.json"
num_cpus=16
time_out=60
mode_gt='gt'
mode_predict='gpt'

python -u ./src/evaluation_ves.py \
    --db_root_path ${db_root_path} \
    --predicted_sql_path ${predicted_sql_path} \
    --data_mode ${data_mode} \
    --ground_truth_path ${ground_truth_path} \
    --num_cpus ${num_cpus} \
    --time_out ${time_out} \
    --mode_gt ${mode_gt} \
    --mode_predict ${mode_predict} \
    --diff_json_path ${diff_json_path}
