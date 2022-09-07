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

get_mineral_relation_suggestion = (
    "SELECT mrs.id, ml.mindat_id as mineral_id, ml_.mindat_id as relation_id, mrs.relation_type_id "
    "FROM mineral_relation_suggestion mrs "
    "INNER JOIN mineral_log ml on ml.id = mrs.mineral_id "
    "INNER JOIN mineral_log ml_ on ml_.id = mrs.relation_id;"
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

get_relations = (
    "SELECT r.rid as id, r.min1 AS mineral_id, r.min2 AS relation_id, r.rel as relation_type_id "
    "FROM relations r;"
)

insert_mineral_log = (
    "INSERT INTO mineral_log AS ml (name, description, mindat_id, ima_symbol) VALUES %s "
    "RETURNING ml.id, ml.name, ml.description, ml.mindat_id, ml.ima_symbol;"
)

insert_mineral_relation_suggestion = (
    "INSERT INTO mineral_relation_suggestion as mrs (id, mineral_id, relation_id, relation_type_id) "
    "SELECT new.id::int, ml.id, ml_.id, new.relation_type_id::int "
    "FROM (VALUES %s) AS new (id, mineral_id, relation_id, relation_type_id) "
    "INNER JOIN mineral_log ml ON ml.mindat_id = new.mineral_id "
    "INNER JOIN mineral_log ml_ ON ml_.mindat_id = new.relation_id "
    "RETURNING mrs.id, mrs.mineral_id, mrs.relation_id, mrs.relation_type_id;"
)

insert_mineral_formula = (
    "WITH ins (id, mineral_id, formula, note, source_id) AS ( "
    "       INSERT INTO mineral_formula AS mf (mineral_id, formula, note, source_id) "
    "       SELECT ml.id, new.formula, new.note, new.source_id "
    "       FROM (VALUES %s) AS new (name, formula, note, source_id) "
    "       INNER JOIN mineral_log AS ml on ml.name = new.name "
    "       RETURNING mf.id, mf.mineral_id, mf.formula, mf.note, mf.source_id, mf.created_at"
    ") "
    "SELECT ml.name, ml.id AS mineral_id, ins.id, ins.formula, ins.note, ins.source_id, ins.created_at "
    "FROM ins "
    "INNER JOIN mineral_log ml ON ml.id = ins.mineral_id;"
)

insert_mineral_history = (
    "WITH ins (id, mineral_id, discovery_year, ima_year, approval_year, publication_year) AS ( "
    "   INSERT INTO mineral_history AS mh (mineral_id, discovery_year, ima_year, approval_year, publication_year) "
    "   SELECT ml.id, new.discovery_year::smallint, new.ima_year::smallint, new.approval_year::smallint, "
    "   new.publication_year::smallint "
    "   FROM (VALUES %s) AS new (name, discovery_year, ima_year, approval_year, publication_year) "
    "   INNER JOIN mineral_log AS ml ON ml.name = new.name "
    "   RETURNING mh.id, mh.mineral_id, mh.discovery_year, mh.ima_year, mh.approval_year, mh.publication_year"
    ") "
    "SELECT ml.name, ml.id AS mineral_id, ins.id, ins.discovery_year, ins.ima_year, ins.approval_year, "
    "       ins.publication_year "
    "FROM ins "
    "INNER JOIN mineral_log ml ON ml.id = ins.mineral_id;"
)


update_mineral_log = (
    "UPDATE mineral_log AS ml SET "
    "description = new.description, "
    "mindat_id = new.mindat_id::int, "
    "ima_symbol = new.ima_symbol "
    "FROM (VALUES %s) AS new (id, description, mindat_id, ima_symbol) "
    "WHERE ml.id::uuid = new.id::uuid "
    "RETURNING ml.id, ml.name, ml.description, ml.mindat_id, ml.ima_symbol;"
)

update_mineral_history = (
    "WITH upd (id, mineral_id, discovery_year, ima_year, approval_year, publication_year) AS ( "
    "UPDATE mineral_history AS mh SET "
    "discovery_year = new.discovery_year::smallint, "
    "ima_year = new.ima_year::smallint, "
    "approval_year = new.approval_year::smallint, "
    "publication_year = new.publication_year::smallint "
    "FROM (VALUES %s) "
    "AS new (id, discovery_year, ima_year, approval_year, publication_year) "
    "WHERE mh.id = new.id "
    "RETURNING mh.id, mh.mineral_id, mh.discovery_year, mh.ima_year, mh.approval_year, mh.publication_year"
    ") "
    "SELECT ml.name, ml.id AS mineral_id, upd.id, upd.discovery_year, upd.ima_year, upd.approval_year, "
    "       upd.publication_year "
    "FROM upd "
    "INNER JOIN mineral_log ml ON ml.id = upd.mineral_id;"
)

update_mineral_relation_suggestion = (
    "UPDATE mineral_relation_suggestion AS mrs SET "
    "mineral_id = ml.id, "
    "relation_id = ml_.id, "
    "relation_type_id = new.relation_type_id, "
    "is_processed = FALSE "
    "FROM (VALUES %s) AS new (id, mineral_id, relation_id, relation_type_id) "
    "INNER JOIN mineral_log ml ON ml.mindat_id = new.mineral_id "
    "INNER JOIN mineral_log ml_ ON ml_.mindat_id = new.relation_id "
    "WHERE mrs.id = new.id "
    "RETURNING mrs.id, mrs.mineral_id, mrs.relation_id, mrs.relation_type_id;"
)

delete_mineral_relation_suggestion = (
    "DELETE FROM mineral_relation_suggestion AS mrs WHERE mrs.id IN "
    "(SELECT old.id FROM (VALUES %s) AS old (id, mineral_id)) "
    "RETURNING mrs.id, mrs.mineral_id, mrs.relation_id, mrs.relation_type_id;"
)
