{{
    config(
        materialized='incremental'
    )
}}

WITH stg_clubs AS (
    SELECT *
    FROM {{ ref('stg_clubs')}}
)

{% if is_incremental() %}

SELECT 		DISTINCT stg.CLUBHASHKEY,
			CURRENT_DATE() AS LOADDATE,
			stg.RECORDSOURCE,
			stg.CLUB_ID AS CLUBID
FROM    	stg_clubs stg
LEFT JOIN 	DBT_PRACTICE.RDV.HUB_CLUB rdv
ON 			stg.CLUBHASHKEY = rdv.CLUBHASHKEY
WHERE 		1 = 1
AND 		rdv.CLUBHASHKEY IS NULL

{% endif %}