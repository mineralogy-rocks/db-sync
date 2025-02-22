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

get_mineral_ima_status = (
    "SELECT ml.name, isl.key AS ima_status "
    "FROM mineral_ima_status mis "
    "INNER JOIN mineral_log ml ON mis.mineral_id = ml.id "
    "INNER JOIN ima_status_list isl on mis.ima_status_id = isl.id;"
)

get_mineral_ima_note = (
    "SELECT ml.name, isl.key AS ima_note "
    "FROM mineral_ima_note min "
    "INNER JOIN mineral_log ml ON min.mineral_id = ml.id "
    "INNER JOIN ima_note_list isl on min.ima_note_id = isl.id;"
)

get_mineral_status = (
    """
    SELECT ml.name, sl.status_id, ms.direct_status
    FROM mineral_status ms
    INNER JOIN mineral_log ml ON ms.mineral_id = ml.id
    INNER JOIN status_list sl ON sl.id = ms.status_id
    WHERE sl.status_group_id IN (2, 3, 4);
    """
)

get_mineral_relation = (
    """
    SELECT ml.name, sl.status_id, ml_.name as relation, ms.direct_status
    FROM mineral_relation mr
    INNER JOIN mineral_log ml ON mr.mineral_id = ml.id
    INNER JOIN mineral_log ml_ ON mr.relation_id = ml_.id
    INNER JOIN mineral_status ms ON mr.mineral_status_id = ms.id
    INNER JOIN status_list sl ON ms.status_id = sl.id;
    """
)

get_mineral_crystallography = (
    "SELECT ml.name, ml.mindat_id, csl.name as crystal_system "
    "FROM mineral_crystallography mc "
    "INNER JOIN mineral_log ml ON mc.mineral_id = ml.id "
    "INNER JOIN crystal_system_list csl ON mc.crystal_system_id = csl.id;"
)

get_mineral_relation_suggestion = (
    "SELECT mrs.id, ml.mindat_id as mineral_id, ml_.mindat_id as relation_id, mrs.relation_type_id "
    "FROM mineral_relation_suggestion mrs "
    "INNER JOIN mineral_log ml on ml.id = mrs.mineral_id "
    "INNER JOIN mineral_log ml_ on ml_.id = mrs.relation_id;"
)

get_minerals = (
    """
        SELECT ml.id AS mindat_id, ml.name AS name, ml.ima_status AS ima_status, ml.ima_notes AS ima_note,
        ml.dispformulasimple AS formula, ml.imaformula as imaformula, ml.formulanotes AS note, ml.imayear AS ima_year,
        ml.yeardiscovery AS discovery_year, ml.approval_year AS approval_year, ml.publication_year AS publication_year,
        ml.description, ml.shortcode_ima AS ima_symbol, ml.csystem as crystal_system, ml2.name as variety_of,
        ml1.name as synonym_of, ml3.name AS polytype_of,
        NULLIF(ml.colour, '') as physical_color,
        NULLIF(ml.diapheny, '') as physical_transparency,
        CAST(NULLIF(NULLIF(ml.dmeas, ''), 0) AS DECIMAL(5,3)) as physical_densityMeasuredMin,
        CAST(NULLIF(NULLIF(ml.dmeas2, ''), 0) AS DECIMAL(5,3)) as physical_densityMeasuredMax,
        CAST(NULLIF(NULLIF(ml.dcalc, ''), 0) AS DECIMAL(5,3)) as physical_densityCalculated,
        CASE WHEN NULLIF(ml.hmin, '') = 0 THEN NULL ELSE NULLIF(ml.hmin, '') END as physical_hardnessMin,
        CASE WHEN NULLIF(ml.hmax, '') = 0 THEN NULL ELSE NULLIF(ml.hmax, '') END as physical_hardnessMax,
        NULLIF(ml.tenacity, '') as physical_tenacity,
        NULLIF(ml.cleavagetype, '') as physical_cleavage,
        NULLIF(ml.cleavage, '') as physical_cleavageNote,
        NULLIF(ml.fracturetype, '') as physical_fracture,
        NULLIF(ml.fracture, '') as physical_fractureNote,
        NULLIF(ml.luminescence, '') as physical_luminescence,
        NULLIF(ml.lustretype, '') as physical_lustre,
        NULLIF(ml.lustre, '') as physical_lustreNote,
        NULLIF(ml.streak, '') as physical_streak,
        NULLIF(ml.opticaltype, '') as optical_type,
        NULLIF(ml.opticalsign, '') as optical_sign,
        NULLIF(ml.opticalextinction, '') as optical_extinction,
        CASE WHEN NULLIF(ml.opticalalpha, '') = 0 THEN NULL ELSE NULLIF(ml.opticalalpha, '') END as optical_alphaMin,
        CASE WHEN NULLIF(ml.opticalalpha2, '') = 0 THEN NULL ELSE NULLIF(ml.opticalalpha2, '') END as optical_alphaMax,
        NULLIF(ml.opticalalphaerror, '') as optical_alphaError,
        CASE WHEN NULLIF(ml.opticalbeta, '') = 0 THEN NULL ELSE NULLIF(ml.opticalbeta, '') END as optical_betaMin,
        CASE WHEN NULLIF(ml.opticalbeta2, '') = 0 THEN NULL ELSE NULLIF(ml.opticalbeta2, '') END as optical_betaMax,
        NULLIF(ml.opticalbetaerror, '') as optical_betaError,
        CASE WHEN NULLIF(ml.opticalgamma, '') = 0 THEN NULL ELSE NULLIF(ml.opticalgamma, '') END as optical_gammaMin,
        CASE WHEN NULLIF(ml.opticalgamma2, '') = 0 THEN NULL ELSE NULLIF(ml.opticalgamma2, '') END as optical_gammaMax,
        NULLIF(ml.opticalgammaerror, '') as optical_gammaError,
        CAST(CASE WHEN NULLIF(ml.opticalomega, '') = 0 THEN NULL ELSE NULLIF(ml.opticalomega, '') END AS DECIMAL(5,3)) as optical_omegaMin,
        CAST(CASE WHEN NULLIF(ml.opticalomega2, '') = 0 THEN NULL ELSE NULLIF(ml.opticalomega2, '') END AS DECIMAL(5,3)) as optical_omegaMax,
        NULLIF(ml.opticalomegaerror, '') as optical_omegaError,
        CAST(CASE WHEN NULLIF(ml.opticalepsilon, '') = 0 THEN NULL ELSE NULLIF(ml.opticalepsilon, '') END AS DECIMAL(5,3)) as optical_epsilonMin,
        CAST(CASE WHEN NULLIF(ml.opticalepsilon2, '') = 0 THEN NULL ELSE NULLIF(ml.opticalepsilon2, '') END AS DECIMAL(5,3)) as optical_epsilonMax,
        NULLIF(ml.opticalepsilonerror, '') as optical_epsilonError,
        CASE WHEN NULLIF(ml.optical2vcalc, '') = 0 THEN NULL ELSE NULLIF(ml.optical2vcalc, '') END as optical_2vMin,
        CASE WHEN NULLIF(ml.optical2vcalc2, '') = 0 THEN NULL ELSE NULLIF(ml.optical2vcalc2, '') END as optical_2vMax,
        NULLIF(ml.optical2vcalcerror, '') as optical_2vError,
        CASE WHEN NULLIF(ml.optical2vmeasured, '') = 0 THEN NULL ELSE NULLIF(ml.optical2vmeasured, '') END as optical_2vMeasuredMin,
        CASE WHEN NULLIF(ml.optical2vmeasured2, '') = 0 THEN NULL ELSE NULLIF(ml.optical2vmeasured2, '') END as optical_2vMeasuredMax,
        NULLIF(ml.optical2vmeasurederror, '') as optical_2vMeasuredError,
        CASE WHEN NULLIF(ml.opticaln, '') = 0 THEN NULL ELSE NULLIF(ml.opticaln, '') END as optical_nMin,
        CASE WHEN NULLIF(ml.opticaln2, '') = 0 THEN NULL ELSE NULLIF(ml.opticaln2, '') END as optical_nMax,
        NULLIF(ml.opticalnerror, '') as optical_nError,
        NULLIF(ml.opticaldispersion, '') as optical_dispersion,
        NULLIF(ml.opticalpleochroism, '') as optical_pleochroism,
        NULLIF(ml.opticalpleochorismdesc, '') as optical_pleochroismNote,
        NULLIF(ml.opticalbirefringence, '') as optical_birefringence,
        NULLIF(ml.opticalcomments, '') as optical_comments,
        NULLIF(ml.opticalcolour, '') as optical_color,
        NULLIF(ml.opticaltropic, '') as optical_tropic,
        NULLIF(ml.opticalanisotropism, '') as optical_anisotropism,
        NULLIF(ml.opticalbireflectance, '') as optical_bireflectance,
        NULLIF(ml.opticalr, '') as optical_r
        FROM minerals ml
        LEFT JOIN minerals ml1 ON ml.synid = ml1.id
        LEFT JOIN minerals ml2 ON ml.varietyof = ml2.id
        LEFT JOIN minerals ml3 ON ml.polytypeof = ml3.id
        WHERE ml.id IN (
            SELECT ml.id
            FROM minerals ml
            WHERE ml.name REGEXP '^[A-Za-z0-9]+'
        );
    """
)

get_relations = (
    "SELECT r.rid as id, r.min1 AS mineral_id, r.min2 AS relation_id, r.rel as relation_type_id "
    "FROM relations r;"
)

get_alternative_names = (
    """
        SELECT _temp.mineral_id, _temp.name, _temp.relation_id, _temp.relation_name,
        CASE WHEN EXISTS (
                SELECT 1
                FROM mineral_status ms
                INNER JOIN status_list sl ON ms.status_id = sl.id AND ms.direct_status AND sl.status_group_id IN (3, 4, 6, 11)
                WHERE ms.mineral_id = _temp.mineral_id
            ) THEN 1
            ELSE _temp.priority
            END AS priority
        FROM (
            WITH RECURSIVE cte(mineral_id, relation_id) AS (
                SELECT
                    mr.mineral_id,
                    mr.relation_id
                FROM mineral_relation mr
                INNER JOIN mineral_status ms ON mr.mineral_status_id = ms.id AND NOT ms.direct_status
                INNER JOIN status_list sl ON ms.status_id = sl.id
                WHERE sl.status_group_id IN (2, 4, 5)
                UNION
                SELECT
                    cte.mineral_id,
                    mr.relation_id
                FROM mineral_relation mr
                INNER JOIN cte ON mr.mineral_id = cte.relation_id
                INNER JOIN mineral_status ms ON mr.mineral_status_id = ms.id AND NOT ms.direct_status
                INNER JOIN status_list sl ON ms.status_id = sl.id
                WHERE sl.status_group_id IN (2, 4, 5)
            )
            SELECT DISTINCT cte.mineral_id, ml.name, cte.relation_id, _ml.name AS relation_name, 2 AS priority FROM cte
            INNER JOIN mineral_log ml ON cte.mineral_id = ml.id
            INNER JOIN mineral_log _ml ON cte.relation_id = _ml.id
            UNION
            SELECT ml.id, ml.name, NULL AS relation_id, NULL AS relation_name, 1 AS priority
            FROM mineral_log ml
            INNER JOIN mineral_status ms ON ml.id = ms.mineral_id AND ms.direct_status
            INNER JOIN status_list sl ON ms.status_id = sl.id AND sl.status_group_id IN (3, 4, 6, 10, 11)
            WHERE NOT EXISTS (
                SELECT 1 FROM mineral_relation mr
                INNER JOIN mineral_status ms ON mr.mineral_status_id = ms.id AND ms.direct_status
                INNER JOIN status_list sl ON ms.status_id = sl.id AND sl.status_group_id IN (2, 4, 5)
                WHERE mr.relation_id = ml.id
            )
            UNION
            SELECT ml.id, ml.name, NULL AS relation_id, NULL AS relation_name, 3 AS priority
            FROM mineral_log ml
            INNER JOIN mineral_status ms ON ml.id = ms.mineral_id AND ms.direct_status
            INNER JOIN status_list sl ON ms.status_id = sl.id
            WHERE NOT EXISTS (
                SELECT 1 FROM mineral_relation mr
                WHERE mr.mineral_id = ml.id OR mr.relation_id = ml.id
            )
        ) _temp
        --WHERE name IN ('Kenotobermorite', 'Papikeite', 'Iodine', 'Oxycalciopyrochlore')
        ORDER BY name;
    """
)

get_cod = (
    """
    SELECT ml.file AS id, axc.ext_id AS amcsd_id, ml.mineral AS mineral_name, ml.a, ml.siga AS a_sigma, ml.b, ml.sigb AS b_sigma,
           ml.c, ml.sigc AS c_sigma, ml.alpha, ml.sigalpha AS alpha_sigma, ml.beta, ml.sigbeta AS beta_sigma,
           ml.gamma, ml.siggamma AS gamma_sigma, ml.vol AS volume, ml.sigvol AS volume_sigma, ml.sg AS space_group,
           ml.formula, ml.calcformula AS calculated_formula,
           CONCAT(CONCAT_WS(
                ' ',
                IF(ml.authors IS NOT NULL, CONCAT(REGEXP_REPLACE(ml.authors, '\\\\.$', ''), '.'), NULL),
                IF(ml.YEAR IS NOT NULL, CONCAT('(', ml.YEAR, ')'), NULL),
                IF(ml.title IS NOT NULL, CONCAT(REGEXP_REPLACE(ml.title, '\\\\.$', ''), '.'), NULL),
                CONCAT_WS(
                    ', ',
                    IF(ml.journal IS NOT NULL, CONCAT('<i>', REGEXP_REPLACE(ml.journal, '\\\\.$', ''), '</i>'), NULL),
                    IF(ml.volume IS NOT NULL, CONCAT('<b>', ml.volume ,'</b>'), NULL),
                    IF(ml.firstpage IS NOT NULL, CONCAT_WS('—', ml.firstpage, ml.lastpage), NULL)
                )
            ), '.') AS reference,
            CASE
            WHEN ml.doi IS NOT NULL
            THEN json_array(
                CONCAT('https://www.crystallography.net/cod/', ml.file, '.html'),
                CONCAT('https://doi.org/', ml.doi)
            )
            ELSE json_array(
                CONCAT('https://www.crystallography.net/cod/', ml.file, '.html')
            )
            END AS links,
           ml.compoundsource AS note
    FROM data ml
    LEFT JOIN amcsd_x_cod axc ON ml.file = axc.cod_id
    WHERE ml.mineral IS NOT NULL;
    """
)

get_chem_measurement = (
    """
    SELECT cm.id, cm.external_key as key
    FROM chem_measurement cm
    WHERE cm.resource = 1
    """
)

insert_chem_measurement = (
    """
        INSERT INTO chem_measurement AS cm (mineral_id, external_key, resource, sample_name, grain_size, rock_name,
                                            rock_texture, alteration, is_primary, tectonic_setting, latitude_min, latitude_max, longitude_min, longitude_max, elevation_min, elevation_max,
                                            citation, location, location_note, created_at)
        SELECT ml.id, new.key, 1, new.sample_name, new.grain_size, new.rock_name, new.rock_texture, new.alteration::int, new.is_primary::bool, new.tectonic_setting,
               new.latitude_min::float, new.latitude_max::float, new.longitude_min::float, new.longitude_max::float, new.elevation_min::float, new.elevation_max::float, new.citation, new.location, new.location_note, CURRENT_TIMESTAMP
        FROM (VALUES %s)
            AS new (key, name, mineral_note, sample_name, grain_size, rock_name, rock_texture, alteration, is_primary, tectonic_setting, citation, latitude_min, latitude_max, longitude_min, longitude_max, elevation_min, elevation_max, location, location_note)
        LEFT JOIN mineral_log AS ml ON UPPER(ml.name) = new.name
        RETURNING cm.id, cm.mineral_id, cm.external_key, cm.resource;
    """
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

insert_mineral_crystallography = (
    "WITH ins (id, mineral_id, crystal_system_id) AS ( "
    "       INSERT INTO mineral_crystallography AS mc (mineral_id, crystal_system_id) "
    "       SELECT ml.id, csl.id "
    "       FROM (VALUES %s) AS new (name, crystal_system) "
    "       INNER JOIN mineral_log AS ml on ml.name = new.name "
    "       INNER JOIN crystal_system_list AS csl on csl.name = new.crystal_system "
    "       RETURNING mc.id, mc.mineral_id, mc.crystal_system_id"
    ") "
    "SELECT ml.name, ml.id AS mineral_id, ins.id, ins.crystal_system_id "
    "FROM ins "
    "INNER JOIN mineral_log ml ON ml.id = ins.mineral_id;"
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

insert_mineral_ima_status = (
    "WITH ins (id, mineral_id, ima_status_id) AS ( "
    "       INSERT INTO mineral_ima_status AS mis (mineral_id, ima_status_id) "
    "       SELECT ml.id, isl.id "
    "       FROM (VALUES %s) AS new (name, ima_status_id) "
    "       INNER JOIN mineral_log AS ml on ml.name = new.name "
    "       INNER JOIN ima_status_list AS isl on isl.key = new.ima_status_id "
    "       RETURNING mis.id, mis.mineral_id, mis.ima_status_id, mis.created_at"
    ") "
    "SELECT ml.name, ml.id AS mineral_id, ins.id, ins.ima_status_id, ins.created_at "
    "FROM ins "
    "INNER JOIN mineral_log ml ON ml.id = ins.mineral_id;"
)

insert_mineral_ima_note = (
    "WITH ins (id, mineral_id, ima_note_id) AS ( "
    "       INSERT INTO mineral_ima_note AS min (mineral_id, ima_note_id) "
    "       SELECT ml.id, inl.id "
    "       FROM (VALUES %s) AS new (name, ima_note) "
    "       INNER JOIN mineral_log AS ml on ml.name = new.name "
    "       INNER JOIN ima_note_list AS inl on inl.key = new.ima_note "
    "       RETURNING min.id, min.mineral_id, min.ima_note_id, min.created_at"
    ") "
    "SELECT ml.name, ml.id AS mineral_id, ins.id, ins.ima_note_id, ins.created_at "
    "FROM ins "
    "INNER JOIN mineral_log ml ON ml.id = ins.mineral_id;"
)

insert_mineral_context = (
    """
    WITH ins (id, mineral_id, data, context_id) AS (
        INSERT INTO mineral_context AS mc (mineral_id, data, context_id)
        SELECT ml.id, new.data::jsonb, dcl.id
        FROM (VALUES %s) AS new (name, data, context_id)
        INNER JOIN mineral_log AS ml ON ml.name = new.name
        INNER JOIN data_context_list AS dcl ON dcl.id = new.context_id
        RETURNING mc.id, mc.mineral_id, mc.data, mc.context_id
    )
    SELECT ml.name, ml.id AS mineral_id, ins.id, ins.data, dcl.name AS context
    FROM ins
    INNER JOIN mineral_log ml ON ml.id = ins.mineral_id
    INNER JOIN data_context_list dcl ON dcl.id = ins.context_id;
    """
)

insert_mineral_structure = (
    """
    WITH ins (id, mineral_id, cod_id, amcsd_id, source_id, a, a_sigma, b, b_sigma, c, c_sigma, alpha, alpha_sigma,
    		  beta, beta_sigma, gamma, gamma_sigma, volume, volume_sigma, space_group, formula, calculated_formula, reference, links,
    		  note) AS (
           INSERT INTO mineral_structure AS ms (mineral_id, cod_id, amcsd_id, source_id, a, a_sigma, b, b_sigma, c, c_sigma, alpha, alpha_sigma,
    		  beta, beta_sigma, gamma, gamma_sigma, volume, volume_sigma, space_group, formula, calculated_formula, reference, links,
    		  note)
           SELECT new.mineral_id::uuid, new.cod_id::int, new.amcsd_id::varchar, new.source_id, new.a::numeric, new.a_sigma::numeric, new.b::numeric, new.b_sigma::numeric, new.c::numeric, new.c_sigma::numeric, new.alpha::numeric, new.alpha_sigma::numeric,
    		  new.beta::numeric, new.beta_sigma::numeric, new.gamma::numeric, new.gamma_sigma::numeric, new.volume::numeric, new.volume_sigma::numeric, new.space_group, new.formula,
    		  new.calculated_formula, new.reference, new.links, new.note
           FROM (VALUES %s) AS new (mineral_id, cod_id, amcsd_id, source_id, a, a_sigma, b, b_sigma, c, c_sigma, alpha, alpha_sigma,
    		  beta, beta_sigma, gamma, gamma_sigma, volume, volume_sigma, space_group, formula, calculated_formula, reference, links,
    		  note)
           RETURNING ms.*
    )
    SELECT ml.name, ins.*
   	FROM ins
    INNER JOIN mineral_log ml ON ml.id = ins.mineral_id;
    """
)

insert_mineral_status = (
    """
    WITH ins (id, mineral_id, status_id) AS (
        INSERT INTO mineral_status AS ms (mineral_id, status_id, needs_revision, direct_status)
        SELECT ml.id, sl.id, TRUE AS needs_revision, new.direct_status
        FROM (VALUES %s) AS new (name, status_id, direct_status)
        INNER JOIN mineral_log AS ml ON ml.name = new.name
        INNER JOIN status_list AS sl ON sl.status_id = new.status_id
        RETURNING ms.id, ms.mineral_id, ms.status_id, ms.needs_revision, ms.direct_status
    )
    SELECT ml.name, ml.id AS mineral_id, ins.id, ins.status_id, ins.needs_revision, ins.direct_status
    FROM ins
    INNER JOIN mineral_log ml ON ml.id = ins.mineral_id;
    """
)

insert_mineral_relation = (
    """
    WITH ins (id, mineral_id, mineral_status_id, relation_id) AS (
        INSERT INTO mineral_relation AS mr (mineral_id, mineral_status_id, relation_id)
        SELECT ml.id, ms.id, ml_.id
        FROM (VALUES %s) AS new (name, status_id, relation, direct_status)
        INNER JOIN mineral_log AS ml ON ml.name = new.name
        INNER JOIN mineral_log AS ml_ ON ml_.name = new.relation
        INNER JOIN status_list AS sl ON sl.status_id = new.status_id
        INNER JOIN mineral_status AS ms ON ms.mineral_id = ml.id AND ms.status_id = sl.id AND
            ms.direct_status = new.direct_status
        RETURNING mr.id, mr.mineral_id, mr.mineral_status_id, mr.relation_id
    )
    SELECT ins.id, ml.name, ins.mineral_status_id, ml_.name
    FROM ins
    INNER JOIN mineral_log ml ON ml.id = ins.mineral_id
    INNER JOIN mineral_log ml_ ON ml_.id = ins.relation_id;
    """
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

update_mineral_crystallography = (
    "WITH upd (id, mineral_id, crystal_system_id) AS ("
    "UPDATE mineral_crystallography AS mc SET "
    "crystal_system_id = csl.id "
    "FROM (VALUES %s) AS new (mineral_name, crystal_system_name) "
    "INNER JOIN mineral_log ml ON ml.name = new.mineral_name "
    "INNER JOIN crystal_system_list csl ON csl.name = new.crystal_system_name "
    "WHERE mc.mineral_id = ml.id "
    "RETURNING mc.id, mc.mineral_id, mc.crystal_system_id"
    ")"
    "SELECT ml.name, ml.id AS mineral_id, upd.id, csl.name as crystal_system_name "
    "FROM upd "
    "INNER JOIN mineral_log ml ON ml.id = upd.mineral_id "
    "INNER JOIN crystal_system_list csl ON upd.crystal_system_id = csl.id;"
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
