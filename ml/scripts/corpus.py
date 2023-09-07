# -*- coding: UTF-8 -*-
import typer
import srsly
import spacy
import warnings
from spacy.tokens import DocBin


def corpus(input_path, output_path):

    # input_path = 'ml/assets/color_train.jsonl'
    # output_path = 'ml/corpus/color_train.spacy'
    # spans_key = 'sc'

    nlp = spacy.blank("en")
    doc_bin = DocBin()
    for text, annotation in srsly.read_json(input_path):
        doc = nlp.make_doc(text)
        ents = []
        for start, end, label in annotation["entities"]:
            span = doc.char_span(start, end, label=label)
            if span is None:
                msg = f"Skipping entity [{start}, {end}, {label}] in the following text because the character span '{doc.text[start:end]}' does not align with token boundaries:\n\n{repr(text)}\n"
                warnings.warn(msg)
            else:
                ents.append(span)
        doc.ents = ents
        doc_bin.add(doc)
    doc_bin.to_disk(output_path)
    print(f"Processed {len(doc_bin)} documents: {output_path}")


if __name__ == "__main__":
    typer.run(corpus)
