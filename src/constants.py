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
    (1, "Almost Totally Altered"),
    (2, "Extensively Altered"),
    (3, "Moderately Altered"),
    (5, "Slightly Altered"),
    (5, "Fresh"),
)

# This is a is_primary bool in database
PRIMARY_SECONDARY_CHOICES = (
    (1, "Primary"),
    (0, "Secondary"),
)

TECTONIC_SETTING_CHOICES = (
    (1, "Archean Craton (Including Greenstone Belts)"),
    (2, "Complex Volcanic Settings"),
    (3, "Continental Flood Basalt"),
    (4, "Convergent Margin"),
    (5, "Intraplate Volcanics"),
    (6, "Ocean Island"),
    (7, "Ocean-Basin Flood Basalt"),
    (8, "Oceanic Plateau"),
    (9, "Rift Volcanics"),
    (10, "Seamount"),
    (11, "Submarine Ridge"),
)


GEOROC_REPLACEMENTS = (
    (
        'TECTONIC SETTING', (
            ('OCEAN ISLANDI', 'OCEAN ISLAND'),
        ),
    ),

    (
        'ALTERATION', (
            ('F', 'Fresh'),
        ),
    ),

    (
        'MINERAL', (
            ('(Al)Kalifeldspar', 'K Feldspar'),
            ('Clay Mineral', 'Clay Minerals'),
            ('Ferri-Tschermakite', 'Ferri-tschermakite'),
            ('Ferri-Tschermakitic Hornblende', 'Ferri-tschermakite'),
            ('Ferricnyb�Ite', 'Ferric-nybøite'),
            ('Magnesio-Hastingsite', 'Magnesio-hastingsite'),
            ('Magnesio-Hornblende', 'Magnesio-hornblende'),
            ('Magnesioarfvedsonite', 'Magnesio-arfvedsonite'),
            ('Moncheite-Merenskyite', 'Melonite Group'),
            ('Selenide', 'Auroselenide'),
            ('Sulfide', 'Sulfide ore'),
            ('Titan-Magnesio-Hastingsite', 'Magnesio-hastingsite'),
            ('Titanite (Sphene)', 'Titanite'),
            ('Titano-Magnetite', 'Titano-magnetite'),
        ),
    ),
)
