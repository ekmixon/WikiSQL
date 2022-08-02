import json
from tqdm import tqdm
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.query import Query
from lib.dbengine import DBEngine


if __name__ == '__main__':
    for split in ['train', 'dev', 'test']:
        print(f'checking {split}')
        engine = DBEngine(f'data/{split}.db')
        n_lines = 0
        with open(f'data/{split}.jsonl') as f:
            for _ in f:
                n_lines += 1
        with open(f'data/{split}.jsonl') as f:
            for l in tqdm(f, total=n_lines):
                d = json.loads(l)
                query = Query.from_dict(d['sql'])

                if not (result := engine.execute_query(d['table_id'], query)):
                    raise Exception(f'Query {query} did not execute to a valid result')
                for a, b, c in d['sql']['conds']:
                    if str(c).lower() not in d['question'].lower():
                        raise Exception(
                            f"Could not find condition {c} in question {d['question']} for query {query}"
                        )
