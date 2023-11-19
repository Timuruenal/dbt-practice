WITH games AS (
			SELECT *
			FROM dbt_practice.landing.games
)
SELECT 
			CAST(GAME_ID 								AS INTEGER) 	AS GAME_ID,
			CAST(COMPETITION_ID 						AS STRING) 		AS COMPETITION_ID,
			CAST(SEASON 								AS INTEGER) 	AS SEASON,
			CAST(ROUND 									AS STRING) 		AS ROUND,
			CAST(DATE 									AS DATE) 		AS DATE,
			CAST(HOME_CLUB_ID 							AS INTEGER) 	AS HOME_CLUB_ID,
			CAST(AWAY_CLUB_ID 							AS INTEGER) 	AS AWAY_CLUB_ID,
			CAST(HOME_CLUB_GOALS 						AS INTEGER) 	AS HOME_CLUB_GOALS,
			CAST(AWAY_CLUB_GOALS 						AS INTEGER) 	AS AWAY_CLUB_GOALS,
			CAST(HOME_CLUB_POSITION 					AS INTEGER) 	AS HOME_CLUB_POSITION,
			CAST(AWAY_CLUB_POSITION 					AS INTEGER) 	AS AWAY_CLUB_POSITION,
			CAST(HOME_CLUB_MANAGER_NAME 				AS STRING) 		AS HOME_CLUB_MANAGER_NAME,
			CAST(AWAY_CLUB_MANAGER_NAME 				AS STRING) 		AS AWAY_CLUB_MANAGER_NAME,
			CAST(STADIUM 								AS STRING) 		AS STADIUM,
			CAST(ATTENDANCE 							AS INTEGER) 	AS ATTENDANCE,
			CAST(REFEREE 								AS STRING) 		AS REFEREE,
			CAST(URL 									AS STRING) 		AS URL,
			CAST(HOME_CLUB_FORMATION 					AS STRING) 		AS HOME_CLUB_FORMATION,
			CAST(AWAY_CLUB_FORMATION 					AS STRING) 		AS AWAY_CLUB_FORMATION,
			CAST(HOME_CLUB_NAME 						AS STRING) 		AS HOME_CLUB_NAME,
			CAST(AWAY_CLUB_NAME 						AS STRING) 		AS AWAY_CLUB_NAME,
			CAST(AGGREGATE 								AS STRING) 		AS AGGREGATE,
			CAST(COMPETITION_TYPE 						AS STRING) 		AS COMPETITION_TYPE,
			CURRENT_DATE() 												AS LoadDate,
			CAST('Transfermarkt'						AS STRING) 		AS RecordSource,
			CAST(MD5(GAME_ID) 							AS CHAR(32)) 	AS GameHashKey,
			MD5(UPPER(CONCAT(
                    TRIM(NVL(COMPETITION_ID            	, '')), '||',
                    TRIM(NVL(SEASON                     , '')), '||',
                    TRIM(NVL(ROUND                      , '')), '||',
                    TRIM(NVL(DATE                       , '')), '||',
                    TRIM(NVL(HOME_CLUB_ID               , '')), '||',
                    TRIM(NVL(AWAY_CLUB_ID               , '')), '||',
                    TRIM(NVL(HOME_CLUB_GOALS            , '')), '||',
                    TRIM(NVL(AWAY_CLUB_GOALS            , '')), '||',
                    TRIM(NVL(HOME_CLUB_POSITION         , '')), '||',
                    TRIM(NVL(AWAY_CLUB_POSITION         , '')), '||',
                    TRIM(NVL(HOME_CLUB_MANAGER_NAME     , '')), '||',
                    TRIM(NVL(AWAY_CLUB_MANAGER_NAME     , '')), '||',
                    TRIM(NVL(STADIUM                    , '')), '||',
                    TRIM(NVL(ATTENDANCE                 , '')), '||',
                    TRIM(NVL(REFEREE                    , '')), '||',
                    TRIM(NVL(URL            			, '')), '||',
                    TRIM(NVL(HOME_CLUB_FORMATION        , '')), '||',
                    TRIM(NVL(AWAY_CLUB_FORMATION        , '')), '||',
                    TRIM(NVL(HOME_CLUB_NAME             , '')), '||',
                    TRIM(NVL(AWAY_CLUB_NAME             , '')), '||',
                    TRIM(NVL(AGGREGATE   				, '')), '||',
                    TRIM(NVL(COMPETITION_TYPE           , ''))
			))) 														AS GameHashDiff
FROM    	games