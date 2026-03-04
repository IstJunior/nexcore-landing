CREATE OR REPLACE VIEW olap.vista_validacion_formularios AS

WITH divisiones AS (
    SELECT
        pd.hash,
        STRING_AGG(DISTINCT CASE WHEN de.tipo_division = 'area' THEN de.nombre_division END, ', ') AS area,
        STRING_AGG(DISTINCT CASE WHEN de.tipo_division = 'subarea' THEN de.nombre_division END, ', ') AS subarea,
        STRING_AGG(DISTINCT CASE WHEN de.tipo_division = 'gerencia' THEN de.nombre_division END, ', ') AS gerencia
    FROM staging.personas_divisiones pd
    LEFT JOIN staging.divisiones_empresa de ON pd.id_division = de.id_division
    GROUP BY pd.hash
)

SELECT
    p.identificacion AS "Identificación",
    p.nombre_completo AS "Nombre",
    p.estado AS "Estado",
    e.nombre_empresa AS "Empresa",
    s.sede AS "Sede",
    nj.nivel_jerarquico AS "Nivel jerárquico",
    d.area AS "Área",
    d.subarea AS "Subárea",
    d.gerencia AS "Gerencia",
    p.cargo AS "Cargo",
    p.jefe_directo AS "Jefe directo",
    i.instrumento AS "Tipo de evaluación",
    NULL::VARCHAR AS "Tipo evaluador",
    NULL::VARCHAR AS "Nombre evaluador",
    ev.fecha_inicio AS "Fecha inicio",
    ev.fecha_reenvio_1 AS "Fecha reenvío 1",
    ev.fecha_reenvio_2 AS "Fecha reenvío 2",
    ev.fecha_finalizacion AS "Fecha finalización",
    ev.estado_evaluacion AS "Estado campaña",
    pec.link_evaluacion AS "Link evaluación",
    pec.estado_participacion AS "Estado participación",
    pec.fecha_respuesta AS "Fecha respuesta",
    NULL::DATE AS "Fecha respuesta evaluador",
    pec.var AS "var",
    p.hash AS "hash",
    pec.id_evaluacion AS "id_evaluacion"

FROM staging.participantes_ev_comun pec
JOIN staging.personas p ON p.hash = pec.hash
JOIN staging.evaluaciones ev ON ev.id_evaluacion = pec.id_evaluacion
JOIN staging.instrumentos i ON i.id_instrumento = ev.id_instrumento
JOIN staging.sedes s ON s.id_sede = p.id_sede
JOIN staging.niveles_jerarquicos nj ON nj.id_nivel_jerarquico = p.id_nivel_jerarquico
JOIN staging.empresas e ON e.id_empresa = s.id_empresa
LEFT JOIN divisiones d ON d.hash = p.hash

UNION ALL

SELECT
    evaluado.identificacion,
    evaluado.nombre_completo,
    evaluado.estado,
    e.nombre_empresa,
    s.sede,
    nj.nivel_jerarquico,
    d.area,
    d.subarea,
    d.gerencia,
    evaluado.cargo,
    evaluado.jefe_directo,
    'Evaluación de desempeño por competencias',
    ped.tipo_evaluador,
    evaluador.nombre_completo,
    ev.fecha_inicio,
    ev.fecha_reenvio_1,
    ev.fecha_reenvio_2,
    ev.fecha_finalizacion,
    ev.estado_evaluacion,
    NULL::TEXT,
    ped.estado_participacion,
    ped.fecha_respuesta,
    NULL::DATE,
    ped.var,
    evaluado.hash,
    ped.id_evaluacion

FROM staging.participantes_evaluacion_desempenio ped
JOIN staging.personas evaluado ON evaluado.hash = ped.hash_evaluado
LEFT JOIN staging.personas evaluador ON evaluador.hash = ped.hash_evaluador
JOIN staging.evaluaciones ev ON ev.id_evaluacion = ped.id_evaluacion
JOIN staging.sedes s ON s.id_sede = evaluado.id_sede
JOIN staging.niveles_jerarquicos nj ON nj.id_nivel_jerarquico = evaluado.id_nivel_jerarquico
JOIN staging.empresas e ON e.id_empresa = s.id_empresa
LEFT JOIN divisiones d ON d.hash = evaluado.hash

UNION ALL

SELECT
    evaluado.identificacion,
    evaluado.nombre_completo,
    evaluado.estado,
    e.nombre_empresa,
    s.sede,
    nj.nivel_jerarquico,
    d.area,
    d.subarea,
    d.gerencia,
    evaluado.cargo,
    evaluado.jefe_directo,
    'Periodo de Prueba',
    ppp.tipo_evaluador,
    evaluador.nombre_completo,
    ppp.fecha_fin_periodo_prueba,
    NULL::DATE,
    NULL::DATE,
    ppp.fecha_fin_periodo_prueba,
    NULL::VARCHAR,
    NULL::TEXT,
    ppp.estado_participacion,
    ppp.fecha_respuesta,
    NULL::DATE,
    ppp.var,
    evaluado.hash,
    NULL::INT

FROM staging.participantes_periodo_prueba ppp
JOIN staging.personas evaluado ON evaluado.hash = ppp.hash
LEFT JOIN staging.personas evaluador ON evaluador.hash = ppp.hash_evaluador
JOIN staging.sedes s ON s.id_sede = evaluado.id_sede
JOIN staging.niveles_jerarquicos nj ON nj.id_nivel_jerarquico = evaluado.id_nivel_jerarquico
JOIN staging.empresas e ON e.id_empresa = s.id_empresa
LEFT JOIN divisiones d ON d.hash = evaluado.hash

UNION ALL

SELECT
    p.identificacion,
    p.nombre_completo,
    p.estado,
    e.nombre_empresa,
    s.sede,
    nj.nivel_jerarquico,
    d.area,
    d.subarea,
    d.gerencia,
    p.cargo,
    p.jefe_directo,
    i.instrumento,
    pn.guias_dispersadas::TEXT,
    NULL::VARCHAR,
    ev.fecha_inicio,
    ev.fecha_reenvio_1,
    ev.fecha_reenvio_2,
    ev.fecha_finalizacion,
    ev.estado_evaluacion,
    NULL::TEXT,
    pn.estado_participacion,
    pn.fecha_respuesta,
    NULL::DATE,
    pn.var,
    p.hash,
    pn.id_evaluacion

FROM staging.participantes_nom pn
JOIN staging.personas p ON p.hash = pn.hash
JOIN staging.evaluaciones ev ON ev.id_evaluacion = pn.id_evaluacion
JOIN staging.instrumentos i ON i.id_instrumento = ev.id_instrumento
JOIN staging.sedes s ON s.id_sede = p.id_sede
JOIN staging.niveles_jerarquicos nj ON nj.id_nivel_jerarquico = p.id_nivel_jerarquico
JOIN staging.empresas e ON e.id_empresa = s.id_empresa
LEFT JOIN divisiones d ON d.hash = p.hash;