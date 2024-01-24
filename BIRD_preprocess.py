import argparse
import json
import os
import pickle
from pathlib import Path
import sqlite3
from tqdm import tqdm
import random

from utils.linking_process import SpiderEncoderV2Preproc, BIRDEncoderV2Preproc
from utils.pretrained_embeddings import GloVe
from utils.datasets.bird import load_tables
# from dataset.process.preprocess_kaggle import gather_questions


def schema_linking_producer(test, train, table, db, dataset_dir):

    # load data
    test_data = json.load(open(os.path.join(dataset_dir, test)))
    train_data = json.load(open(os.path.join(dataset_dir, train)))

    #load db name 
    
    # load schemas
    schemas, eval_foreign_key_maps = load_tables( [os.path.join(dataset_dir, table) ])
    test_dbs = os.listdir()
    # Backup in-memory copies of all the DBs and create the live connections
    for db_id, schema in tqdm(schemas.items(), desc="DB connections"):
        sqlite_path = Path(dataset_dir) / db / db_id / f"{db_id}.sqlite"
        source: sqlite3.Connection
        with sqlite3.connect(str(sqlite_path)) as source:
            dest = sqlite3.connect(':memory:')
            dest.row_factory = sqlite3.Row
            source.backup(dest)
        schema.connection = dest

    word_emb = GloVe(kind='42B', lemmatize=True)
    linking_processor = BIRDEncoderV2Preproc(dataset_dir,
            min_freq=4,
            max_count=5000,
            include_table_name_in_column=False,
            word_emb=word_emb,
            fix_issue_16_primary_keys=True,
            compute_sc_link=True,
            compute_cv_link=True)
    print("build schema-linking ... ")

    # build schema-linking
    for data, section in zip([test_data, train_data],['test', 'train']):
        if section == 'test':
            continue  
        for idx , item in enumerate(tqdm(data, desc=f"{section} section linking")):
            if idx < 5798:
                continue
            db_id = item["db_id"]
            schema = schemas[db_id]
            # print(len(schema.columns))
            to_add, validation_info = linking_processor.validate_item(item, schema, section)
            print("VALIDEATE RES:",  to_add)
            if to_add:
                linking_processor.add_item(item, schema, section, validation_info)
    print("Coming save ... ")
    # save
    linking_processor.save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, default="./dataset/bird")
    args = parser.parse_args()
    # schema-linking between questions and databases for Spider
    bird_dev = "dev/dev.json"
    bird_train = 'train/train.json'
    bird_table =  "tables.json"
    bird_db = "database"
    bird_dir = args.data_dir
    schema_linking_producer(bird_dev, bird_train, bird_table, bird_db, bird_dir)


