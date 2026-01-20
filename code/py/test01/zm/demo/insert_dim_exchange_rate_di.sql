INSERT OVERWRITE TABLE dim.dim_exchange_rate_di
WITH 
-- 1. 处理汇率表(a表) - 取各分组最新数据
latest_rate AS (
    SELECT 
        a.kurst,
        a.fcurr,
        a.tcurr,
        a.gdatu,
        a.ukurs,
        a.insert_dt,
        ROW_NUMBER() OVER (
            PARTITION BY a.kurst, a.fcurr, a.tcurr 
            ORDER BY a.gdatu DESC
        ) AS rn
    FROM ods.ods_sap_erp_tcurr_df a 
    WHERE a.kurst IN ('M', 'EURX', 'PEND')
      -- 取昨天和今天有效期内数据
      AND (a.gdatu <= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d')
           OR a.gdatu <= DATE_FORMAT(CURRENT_DATE(), '%Y%m%d'))
),

-- 2. 处理汇率转换因子表(b表) - 取各分组最新数据
latest_factor AS (
    SELECT 
        b.kurst,
        b.fcurr,
        b.tcurr,
        b.ffact,
        b.tfact,
        ROW_NUMBER() OVER (
            PARTITION BY b.kurst, b.fcurr, b.tcurr 
            ORDER BY b.gdatu DESC
        ) AS rn
    FROM ods.ods_sap_erp_tcurf_df b 
    WHERE b.kurst IN ('M', 'EURX', 'PEND')
      -- 取昨天和今天有效期内数据
      AND (b.gdatu <= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d')
           OR b.gdatu <= DATE_FORMAT(CURRENT_DATE(), '%Y%m%d'))
),

-- 3. 获取最终数据（为昨天和今天分别生成数据）
final_data AS (
    -- 为昨天生成数据
    SELECT 
        DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d') AS dt,
        lr.kurst AS rate_type,
        lr.fcurr AS from_ccy,
        lr.tcurr AS to_ccy,
        lr.gdatu AS start_date,
        CAST(lr.ukurs AS DECIMAL(27,8)) AS raw_rate,
        CAST(lf.ffact AS DECIMAL(27,8)) AS from_unit_rate,
        CAST(lf.tfact AS DECIMAL(27,8)) AS to_unit_rate,
        CASE 
            WHEN lf.ffact IS NOT NULL AND lf.ffact != 0 
            THEN CAST(lr.ukurs * lf.tfact / lf.ffact AS DECIMAL(27,8))
            ELSE CAST(lr.ukurs AS DECIMAL(27,8))
        END AS final_rate,
        lr.insert_dt
    FROM latest_rate lr
    LEFT JOIN latest_factor lf 
        ON lr.kurst = lf.kurst 
        AND lr.fcurr = lf.fcurr 
        AND lr.tcurr = lf.tcurr
        AND lf.rn = 1
    WHERE lr.rn = 1
      AND lr.gdatu <= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d')
    
    UNION ALL
    
    -- 为今天生成数据
    SELECT 
        DATE_FORMAT(CURRENT_DATE(), '%Y%m%d') AS dt,
        lr.kurst AS rate_type,
        lr.fcurr AS from_ccy,
        lr.tcurr AS to_ccy,
        lr.gdatu AS start_date,
        CAST(lr.ukurs AS DECIMAL(27,8)) AS raw_rate,
        CAST(lf.ffact AS DECIMAL(27,8)) AS from_unit_rate,
        CAST(lf.tfact AS DECIMAL(27,8)) AS to_unit_rate,
        CASE 
            WHEN lf.ffact IS NOT NULL AND lf.ffact != 0 
            THEN CAST(lr.ukurs * lf.tfact / lf.ffact AS DECIMAL(27,8))
            ELSE CAST(lr.ukurs AS DECIMAL(27,8))
        END AS final_rate,
        lr.insert_dt
    FROM latest_rate lr
    LEFT JOIN latest_factor lf 
        ON lr.kurst = lf.kurst 
        AND lr.fcurr = lf.fcurr 
        AND lr.tcurr = lf.tcurr
        AND lf.rn = 1
    WHERE lr.rn = 1
      AND lr.gdatu <= DATE_FORMAT(CURRENT_DATE(), '%Y%m%d')
)

-- 4. 插入数据
SELECT 
    dt,
    rate_type,
    from_ccy,
    to_ccy,
    start_date,
    raw_rate,
    from_unit_rate,
    to_unit_rate,
    final_rate,
    insert_dt
FROM final_data
WHERE dt IS NOT NULL 
  AND rate_type IS NOT NULL 
  AND from_ccy IS NOT NULL 
  AND to_ccy IS NOT NULL
;