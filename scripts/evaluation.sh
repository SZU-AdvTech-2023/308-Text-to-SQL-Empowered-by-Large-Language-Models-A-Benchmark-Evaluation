
python  get_final_spider.py --root_path ./history/spider_question_sql_opensql/

python ./src/evaluation_spider.py --gold /public24_data/qth/dataset/spider/dev_gold.sql \
                     --pred /public24_data/qth/DAIL-SQL/history/spider_question_sql_opensql/predict.txt \
                    --etype all \
                    --db /public24_data/qth/dataset/spider/database  \
                    --table /public24_data/qth/dataset/spider/tables.json