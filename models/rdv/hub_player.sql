{{
    config(
        materialized='incremental'
    )
}}

WITH stg_players AS (
    SELECT *
    FROM {{ ref('stg_players')}}
)

{% if is_incremental() %}

SELECT 		DISTINCT stg.PLAYERHASHKEY,
			CURRENT_DATE() AS LOADDATE,
			stg.RECORDSOURCE,
			stg.PLAYER_ID AS PLAYERID
FROM    	stg_players stg
LEFT JOIN 	DBT_PRACTICE.RDV.HUB_PLAYER rdv
ON 			stg.PLAYERHASHKEY = rdv.PLAYERHASHKEY
WHERE 		1 = 1
AND 		rdv.PLAYERHASHKEY IS NULL

{% endif %}