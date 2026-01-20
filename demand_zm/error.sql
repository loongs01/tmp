WITH base_data AS (
    SELECT DISTINCT 
        matnr,
        idnrk_s,
        idnrk,
        alprf
    FROM ods.ods_sap_erp_substitutmaterials_test
),

standard_materials AS (
    SELECT 
        matnr,
        idnrk_s,
        ROW_NUMBER() OVER (PARTITION BY matnr ORDER BY idnrk_s) AS mat_seq,
        COUNT(*) OVER (PARTITION BY matnr) AS total_mats
    FROM (
        SELECT DISTINCT 
            matnr,
            idnrk_s
        FROM base_data
    ) t
),
--select * from standard_materials

material_alternatives_prep AS (
    SELECT 
        *
    FROM base_data sm
    left JOIN standard_materials bd ON sm.matnr = bd.matnr AND sm.idnrk_s = bd.idnrk_s

--     FROM standard_materials sm
--     left JOIN base_data bd ON sm.matnr = bd.matnr AND sm.idnrk_s = bd.idnrk_s
) select * from material_alternatives_prep

结果
matnr
idnrk_s
idnrk
alprf
matnr_1
idnrk_s_1
mat_seq
total_mats
A01
M04
M04-1
1
A01
M03
M03
-1
A01
M05
M05-1
1
A01
M05
M05-2
2
A01
M05
M05
-1
A01
M04
M04
-1
A01
M03
M03-1
1
而如下查询
WITH base_data AS (
    SELECT DISTINCT 
        matnr,
        idnrk_s,
        idnrk,
        alprf
    FROM ods.ods_sap_erp_substitutmaterials_test
),

standard_materials AS (
    SELECT 
        matnr,
        idnrk_s,
        ROW_NUMBER() OVER (PARTITION BY matnr ORDER BY idnrk_s) AS mat_seq,
        COUNT(*) OVER (PARTITION BY matnr) AS total_mats
    FROM (
        SELECT DISTINCT 
            matnr,
            idnrk_s
        FROM base_data
    ) t
),
--select * from standard_materials

material_alternatives_prep AS (
    SELECT 
        *
--     FROM base_data sm
--     left JOIN standard_materials bd ON sm.matnr = bd.matnr AND sm.idnrk_s = bd.idnrk_s

    FROM standard_materials sm
    left JOIN base_data bd ON sm.matnr = bd.matnr AND sm.idnrk_s = bd.idnrk_s
) select * from material_alternatives_prep

matnr
idnrk_s
mat_seq
total_mats
matnr_1
idnrk_s_1
idnrk
alprf
A01
M03
1
3
A01
M05
3
3
A01
M04
2
3

