python generate_question.py \
--data_type bird \
--split test \
--tokenizer gpt-3.5-turbo \
--max_seq_len 4096 \
--selector_type EUCDISMASKPRESKLSIMTHR \
--pre_test_result  ./dataset/process/BIRD-TEST_OPENSQL_9-SHOT_EUCDISQUESTIONMASK_QA-EXAMPLE_CTX-200_ANS-4096/output_sql.txt \
--prompt_repr OPENSQL \
--k_shot 9 \
--example_type QA