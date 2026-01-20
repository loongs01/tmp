WITH a_dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY rate_type, from_ccy, to_ccy ORDER BY gdatu DESC) AS rn
    FROM ods_sap_erp_tcurr_df /* filter/where conditions may be needed */
  ) t WHERE rn = 1
),
b_dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY rate_type, from_ccy, to_ccy ORDER BY gdatu DESC) AS rn
    FROM ods_sap_erp_tcurf_df /* filter/where conditions may be needed */
  ) t WHERE rn = 1
)
INSERT INTO dim_exchange_rate_di (`dt`, `rate_type`, `from_ccy`, `to_ccy`, `start_date`, `raw_rate`, `from_unit_rate`, `by`, `to_unit_rate`, `final_rate`, `insert_dt`)
SELECT CAST(current_date AS VARCHAR) AS dt /* YYYYMMDD formatting may be required */, a_dedup.kurst AS rate_type, a_dedup.fcurr AS from_ccy, a_dedup.tcurr AS to_ccy, a_dedup.gdatu AS start_date, a_dedup.ukurs AS raw_rate, b_dedup.ffact AS from_unit_rate, NULL AS by /* TODO: map source */, b_dedup.tfact AS to_unit_rate, (a.ukurs * b.tfact / b.ffact) AS final_rate, a_dedup.insert_dt AS insert_dt
FROM a_dedup a LEFT JOIN b_dedup b ON a.rate_type = b.rate_type AND a.fcurr = b.fcurr AND a.tcurr = b.tcurr;