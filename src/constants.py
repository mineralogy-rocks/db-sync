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
    ('silver', 'silver'),
    ('cream', 'cream'),
    # ('colorless', 'colou?rless'),
]

ALTERATION_CHOICES = (
    (1, "Almost totally altered"),
    (2, "Extensively altered"),
    (3, "Fresh"),
    (4, "Moderately altered"),
    (5, "Slightly altered"),
)

PRIMARY_SECONDARY_CHOICES = (
    (1, "Primary"),
    (2, "Secondary"),
)

TECTONIC_SETTING_CHOICES = (
    (1, "Archean Craton (including Greenstone Belts)"),
    (2, "Complex Volcanic Settings"),
    (3, "Continental Flood Basalt"),
    (4, "Convergent Margin"),
    (5, "Intraplate Volcanics"),
    (6, "Ocean Island"),
    (7, "Ocean-basin Flood Basalt"),
    (8, "Oceanic Plateau"),
    (9, "Rift Volcanics"),
    (10, "Seamount"),
    (11, "Submarine Ridge"),
)
