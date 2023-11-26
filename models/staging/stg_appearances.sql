WITH appearances AS (
SELECT 		*
FROM 		{{ source('transfermarkt', 'appearances')}}
)
SELECT 		
			CAST(APPEARANCE_ID 								AS STRING) 		AS APPEARANCE_ID,
			CAST(GAME_ID 									AS INTEGER) 	AS GAME_ID,
			CAST(PLAYER_ID									AS INTEGER) 	AS PLAYER_ID,
			CAST(PLAYER_CLUB_ID								AS INTEGER) 	AS PLAYER_CLUB_ID,
			CAST(PLAYER_CURRENT_CLUB_ID 					AS INTEGER) 	AS PLAYER_CURRENT_CLUB_ID,
			CAST(DATE										AS DATE) 		AS DATE,
			CAST(PLAYER_NAME 								AS STRING) 		AS PLAYER_NAME,
			CAST(COMPETITION_ID								AS STRING) 		AS COMPETITION_ID,
			CAST(YELLOW_CARDS								AS INTEGER) 	AS YELLOW_CARDS,
			CAST(RED_CARDS									AS INTEGER) 	AS RED_CARDS,
			CAST(GOALS										AS INTEGER) 	AS GOALS,
			CAST(ASSISTS									AS INTEGER) 	AS ASSISTS,
			CAST(MINUTES_PLAYED								AS INTEGER) 	AS MINUTES_PLAYED,
			CURRENT_DATE() 													AS LoadDate,
			CAST('Transfermarkt'							AS STRING) 		AS RecordSource,
			CAST(MD5(APPEARANCE_ID) 						AS CHAR(32)) 	AS AppearanceHashKey,
			MD5(UPPER(CONCAT(
                    TRIM(NVL(GAME_ID                        , '')), '||',
                    TRIM(NVL(PLAYER_ID                      , '')), '||',
                    TRIM(NVL(PLAYER_CLUB_ID                 , '')), '||',
                    TRIM(NVL(PLAYER_CURRENT_CLUB_ID         , '')), '||',
                    TRIM(NVL(DATE                           , '')), '||',
                    TRIM(NVL(PLAYER_NAME                    , '')), '||',
                    TRIM(NVL(COMPETITION_ID                 , '')), '||',
                    TRIM(NVL(YELLOW_CARDS                   , '')), '||',
                    TRIM(NVL(RED_CARDS                      , '')), '||',
                    TRIM(NVL(GOALS                 			, '')), '||',
                    TRIM(NVL(ASSISTS                        , '')), '||',
                    TRIM(NVL(MINUTES_PLAYED                 , ''))
			))) 															AS AppearanceHashDiff
FROM 		appearances   