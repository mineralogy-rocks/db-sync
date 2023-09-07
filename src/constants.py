# -*- coding: UTF-8 -*-
STATUS_SYNONYM_UNCERTAIN = 2.11
STATUS_UNCERTAIN_VARIETY = 4.05
STATUS_POLYTYPE = 3

IMA_STATUS_CHOICES = {
    "APPROVED": 1,
    "DISCREDITED": 2,
    "PENDING_PUBLICATION": 4,
    "GRANDFATHERED": 8,
    "QUESTIONABLE": 16,
}

IMA_NOTES_CHOICES = {
    "REJECTED": 1,
    "PENDING_APPROVAL": 2,
    "GROUP": 4,
    "REDEFINED": 8,
    "RENAMED": 16,
    "INTERMEDIATE": 32,
    "PUBLISHED_WITHOUT_APPROVAL": 64,
    "UNNAMED_VALID": 128,
    "UNNAMED_INVALID": 256,
    "NAMED_AMPHIBOLE": 512,
}

BASE_COLORS_MAP = [
    ('black', 'black'),
    ('blue', 'blu'),
    ('brown', 'brown'),
    ('green', 'green'),
    ('grey', 'gr[ae]y'),
    ('orange', 'orang'),
    ('pink', 'pink'),
    ('purple', 'purpl'),
    ('red', 'red'),
    ('white', 'whit'),
    ('yellow', 'yellow'),
    ('violet', 'violet'),
    ('indigo', 'indig'),
    ('cyan', 'cyan'),
    ('magenta', 'magent'),
    ('turquoise', 'turquois'),
    ('azure', 'azur'),
    ('beige', 'beig'),
    ('coral', 'coral'),
    ('gold', 'gold'),
    ('cream', 'cream'),
    ('colorless', 'colou?rless'),
]
