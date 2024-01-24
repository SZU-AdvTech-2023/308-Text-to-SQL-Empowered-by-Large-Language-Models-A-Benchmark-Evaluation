from openai_parallel_toolkit import ParallelToolkit, OpenAIModel
import os 

root_path = "./dataset/process/BIRD-TEST_OPENSQL_9-SHOT_EUCDISMASKPRESKLSIMTHR_QA-EXAMPLE_CTX-200_ANS-4096"
input_path= os.path.join(root_path, "input.jsonl")
output_path= os.path.join(root_path, "output.jsonl")
if os.path.exists(input_path):
    if  not os.path.exists(output_path):
        f = open(output_path, 'w')
        f.close()
    config_path = "config5.json"
    model = OpenAIModel(model_name="gpt-3.5-turbo-16k", temperature=0)
    tool = ParallelToolkit(config_path=config_path,
                                    openai_model=model,
                                    input_path=input_path,
                                    output_path=output_path)
    tool.run() 