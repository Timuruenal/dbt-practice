{{
    config(
        materialized='incremental'
    )
}}

WITH stg_games AS (
    SELECT *
    FROM {{ ref('stg_games')}}
)

{% if is_incremental() %}

SELECT 		DISTINCT stg.GAMEHASHKEY,
			CURRENT_DATE() AS LOADDATE,
			stg.RECORDSOURCE,
			stg.GAME_ID AS GAMEID
FROM    	stg_games stg
LEFT JOIN 	DBT_PRACTICE.RDV.HUB_GAME rdv
ON 			stg.GAMEHASHKEY = rdv.GAMEHASHKEY
WHERE 		1 = 1
AND 		rdv.GAMEHASHKEY IS NULL

{% endif %}