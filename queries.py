import os

bq_table = os.getenv('BIGQUERY_TABLE')

billing_query = f"""
    SELECT 
        service.description AS service,
        project.name AS `project`,
        sku.description AS sku,
        ROUND(SUM(cost), 2) AS cost
    FROM 
        `{bq_table}`
    WHERE 
        DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) 
    GROUP BY 1,2,3
    ORDER BY cost DESC
"""

billing_query_services = f"""
WITH q1 AS (
    SELECT 
        DATE(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) AS day,
        service.description AS service,
        --sku.description AS sku,
        SUM(cost) AS cost
    FROM 
        `{bq_table}`
    WHERE 
        DATE(_PARTITIONTIME) BETWEEN 
            DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)
            AND DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
    GROUP BY 1,2--,3
    ORDER BY cost DESC
),

-- group_service_sku AS (
--   SELECT 
--     service,
--     sku,
--     SUM(IF(day = DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY), cost, 0)) cost_1d,
--     AVG(cost) AS avg_7d,
--     MAX(cost) AS max_7d
--   FROM q1
--   GROUP BY 1,2
-- ),

group_service AS (
  SELECT 
    service,
    ROUND(SUM(IF(day = DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY), cost, 0)), 2) cost_1d,
    ROUND(SAFE_DIVIDE(
      SUM(IF(day = DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY), cost, 0)),
      AVG(cost)
    )*100 - 100, 2) AS perc_avg,
    ROUND(AVG(cost), 2) AS avg_7d,
    ROUND(MAX(cost), 2) AS max_7d
  FROM q1
  GROUP BY 1
  ORDER BY 2 DESC
)

SELECT * FROM group_service

"""