# -*- coding: UTF-8 -*-
import re
from collections import Counter
from typing_extensions import Annotated, deprecated

import pandas as pd
import spacy
from spacy.pipeline.spancat import DEFAULT_SPANCAT_MODEL


# Convert csv into txt
train = pd.read_csv('ml/assets/color_train.csv', names=['text'])
train['text'] = train['text'].str.strip()
train['text'] = train['text'].str.lower()
train = train['text']
train.to_csv('ml/assets/color_train.txt', sep='\t', index=False, header=False)

test = pd.read_csv('ml/assets/color_test.csv', names=['text'])
test['text'] = test['text'].str.strip()
test['text'] = test['text'].str.lower()
test = test['text']
test.to_csv('ml/assets/color_dev.txt', sep='\t', index=False, header=False)


# Load ner model from training/model-best
nlp = spacy.load('ml/training/model-best')
span_key = 'sc'

# TEST NER model
doc = nlp("Black, dark-green, greenish-brown, yellow")
print("Entities", [(ent.text, ent.label_) for ent in doc.ents])

# print("Entities", [(ent.text, ent.label_) for ent in doc.ents])

_text = "mustard yellow to brownish yellow; Black or dark blue-green; Blue-grey; Bluish black; Bluish-black to black.; Bluish-black to black, purple; Bluish green; Bluish-green to green.; Bluish grey; Bright green to emerald green; Brown; Brownish black; Brownish-black; Brownish yellow; Brown to brownish-red, rose-red, or yellow, grey-brown, and also pale to dark green. dark green blue and grey blue; Brown to brownish-red, rose-red, yellow, grey-brown, also pale to dark green.; Cherry red to very dark red; Clove-brown to dark brown, grey to white, green."
_text = _text.lower()
_texts = re.split(r'[;,.]', _text)
_texts = [t.strip() for t in _texts if t.strip()]

for _text in _texts:
    doc = nlp(_text)
    print("Entities", [(ent.text, ent.label_) for ent in doc.ents])
    Counter([ent.text for ent in doc.ents])

# SAVE NER model
nlp.to_disk("ml/color_ner_model")

# LOAD NER model
nlp = spacy.load("ml/color_ner_model")

# remove training data
nlp.remove_pipe("spancat")
