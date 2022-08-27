# -*- coding: UTF-8 -*-
get_mineral_log = (
    "SELECT ml.id, ml.name, ml.note, ml.created_at, ml.updated_at, ml.seen, ml.description, "
    "ml.mindat_id, ml.ima_symbol FROM mineral_log ml;"
)

get_mineral_history = (
    "SELECT mh.id, ml.name, mh.discovery_year, mh.ima_year, mh.approval_year, mh.publication_year "
    "FROM mineral_history mh "
    "INNER JOIN mineral_log ml on mh.mineral_id = ml.id;"
)

get_mineral_formula = (
    "SELECT ml.name, ml.mindat_id, mf.formula, mf.note, mf.source_id "
    "FROM mineral_formula mf "
    "INNER JOIN mineral_log ml ON mf.mineral_id = ml.id "
    "WHERE mf.source_id > 1;"
)

get_minerals = (
    "SELECT ml.id AS mindat_id, ml.name AS name, ml.formula, ml.imaformula as imaformula, ml.formulanotes AS note, "
    "ml.imayear AS ima_year, "
    "ml.yeardiscovery AS discovery_year, ml.approval_year AS approval_year, ml.publication_year AS publication_year, "
    "ml.description, ml.shortcode_ima AS ima_symbol "
    "FROM minerals ml "
    "WHERE ml.id IN ( "
    "    SELECT ml.id "
    "    FROM minerals ml "
    "    WHERE ml.name REGEXP '^[A-Za-z0-9]+'"
    ");"
)

insert_mineral_log = (
    "INSERT INTO mineral_log (name, description, mindat_id, ima_symbol) VALUES %s;"
)

insert_mineral_history = (
    "INSERT INTO mineral_history AS mh (mineral_id, discovery_year, ima_year, approval_year, publication_year) "
    "SELECT ml.id, new.discovery_year::smallint, new.ima_year::smallint, new.approval_year::smallint, "
    "new.publication_year::smallint "
    "FROM (VALUES %s) AS new (name, discovery_year, ima_year, approval_year, publication_year) "
    "INNER JOIN mineral_log AS ml ON ml.name = new.name "
    "RETURNING mh.id, mh.discovery_year, mh.ima_year, mh.approval_year, mh.publication_year;"
)


update_mineral_log = (
    "UPDATE mineral_log AS ml SET "
    "description = new.description, "
    "mindat_id = new.mindat_id, "
    "ima_symbol = new.ima_symbol "
    "FROM (VALUES %s) AS new (id, description, mindat_id, ima_symbol) "
    "WHERE ml.id::uuid = new.id::uuid;"
)

update_mineral_history = (
    "UPDATE mineral_history AS mh SET "
    "discovery_year = new.discovery_year::smallint, "
    "ima_year = new.ima_year::smallint, "
    "approval_year = new.approval_year::smallint, "
    "publication_year = new.publication_year::smallint "
    "FROM (VALUES %s) "
    "AS new (id, discovery_year, ima_year, approval_year, publication_year) "
    "WHERE mh.id = new.id "
    "RETURNING mh.id, mh.discovery_year, mh.ima_year, mh.approval_year, mh.publication_year;"
)
