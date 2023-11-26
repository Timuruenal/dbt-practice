{{
    config(
        materialized='incremental'
    )
}}

WITH stg_appearances AS (
    SELECT *
    FROM {{ ref('stg_appearances')}}
)

{% if is_incremental() %}

SELECT 		DISTINCT stg.APPEARANCEHASHKEY,
			CURRENT_DATE() AS LOADDATE,
			stg.RECORDSOURCE,
			stg.APPEARANCE_ID AS APPEARANCEID
FROM    	stg_appearances stg
LEFT JOIN 	DBT_PRACTICE.RDV.HUB_APPEARANCE rdv
ON 			stg.APPEARANCEHASHKEY = rdv.APPEARANCEHASHKEY
WHERE 		1 = 1
AND 		rdv.APPEARANCEHASHKEY IS NULL

{% endif %}