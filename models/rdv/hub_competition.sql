{{
    config(
        materialized='incremental'
    )
}}

WITH stg_competitions AS (
    SELECT *
    FROM {{ ref('stg_competitions')}}
)

{% if is_incremental() %}

SELECT 		DISTINCT stg.COMPETITIONHASHKEY,
			CURRENT_DATE() AS LOADDATE,
			stg.RECORDSOURCE,
			stg.COMPETITION_ID AS COMPETITIONID
FROM    	stg_competitions stg
LEFT JOIN 	DBT_PRACTICE.RDV.HUB_COMPETITION rdv
ON 			stg.COMPETITIONHASHKEY = rdv.COMPETITIONHASHKEY
WHERE 		1 = 1
AND 		rdv.COMPETITIONHASHKEY IS NULL

{% endif %}