# -*- coding: UTF-8 -*-
import json
from pathlib import Path

import pandas as pd
import typer
import srsly


def preprocess(input_path, output_path):
    '''
    Merge annotations into a single file and convert entities to spans
    '''
    # input_path = 'ml/assets/color_train.json'
    # output_path = 'ml/assets/color_train.jsonl'
    # pattern = 'color_train'
    # spans_key = 'sc'

    with open(input_path, 'r') as f:
        data = json.load(f)

    srsly.write_jsonl(output_path, [data['annotations']])

    # below is code for running with SpanCategorizer

    # path_ = Path(input_path)
    # files = []
    # for file in path_.iterdir():
    #     if file.name.startswith(pattern) and file.name.endswith('.json'):
    #         files.append(file)
    #
    # merge = pd.DataFrame(columns=['text', 'spans'])
    # for file in files:
    #     with open(file, 'r') as f:
    #         data = json.load(f)
    #
    #     parsed = []
    #     for index, (annotation, entities) in enumerate(data['annotations']):
    #         spans = []
    #         for entity in entities['entities']:
    #             start = entity[0]
    #             end = entity[1]
    #             label = entity[2]
    #             spans.append((start, end, label))
    #         parsed.append((annotation, spans))
    #     merge = pd.concat([merge, pd.DataFrame(parsed, columns=['text', 'spans'])])
    #
    # merge = merge.groupby('text')['spans'].apply(lambda x: x.tolist()).reset_index()
    # merge['spans'] = merge['spans'].apply(lambda x: {'spans': { spans_key: [item for sublist in x for item in sublist] }})
    # output = []
    # for index, row in merge.iterrows():
    #     output.append((row['text'], row['spans']))
    #
    # srsly.write_jsonl(output_path, [output])


if __name__ == "__main__":
    typer.run(preprocess)
