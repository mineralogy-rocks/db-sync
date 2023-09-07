# -*- coding: UTF-8 -*-
import re
from collections import defaultdict

import pandas as pd
import spacy

from src.constants import BASE_COLORS_MAP

nlp = spacy.load("en_colorExtractor")
# Convert csv into txt
# train = pd.read_csv('ml/assets/color_train.csv', names=['text'])
# train['text'] = train['text'].str.strip()
# train['text'] = train['text'].str.lower()
# train = train['text']
# train.to_csv('ml/assets/color_train.txt', sep='\t', index=False, header=False)
#
# test = pd.read_csv('ml/assets/color_test.csv', names=['text'])
# test['text'] = test['text'].str.strip()
# test['text'] = test['text'].str.lower()
# test = test['text']
# test.to_csv('ml/assets/color_dev.txt', sep='\t', index=False, header=False)
#
def recognize_colors(notes):
    BASE_COLORS_REGEX = '|'.join([f'(?P<{color}>{_}\w*)' for color, _ in BASE_COLORS_MAP])
    colors = defaultdict(list)

    # notes = 'colorless, colourless, Brown to brownish-red, rose-red, or yellow, grey-brown, and also pale to dark green, pale green and also greenish'
    notes = notes.lower()
    notes = re.split(r'[;.]', notes)
    notes = [t.strip() for t in notes if t.strip()]
    notes = [re.sub(r'\s+', ' ', t) for t in notes if t.strip()]
    notes = ', '.join(notes)
    doc = nlp(notes)

    for _entity in doc.ents:
        _entities = re.split(r'[-\s]', _entity.text)
        last_word = _entities[-1]
        match = re.match(BASE_COLORS_REGEX, last_word)
        if match:
            _color = match.lastgroup
            if _entity.text != _color:
                colors[_color].append(_entity.text)
            else:
                colors[_color]
        else:
            colors['other'].append(_entity.text)

    colors_list = [{'primaryColor': color, 'entities': entities} for color, entities in colors.items()]
    # print(doc.ents)
    # print(colors_list)

    return colors_list
