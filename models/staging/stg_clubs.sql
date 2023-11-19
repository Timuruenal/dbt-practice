WITH clubs AS (
SELECT 		*
FROM 		dbt_practice.landing.clubs
)
SELECT 		
			CAST(CLUB_ID 									AS INTEGER) 	AS CLUB_ID,
			CAST(CLUB_CODE 									AS STRING) 		AS CLUB_CODE,
			CAST(NAME										AS STRING) 		AS NAME,
			CAST(DOMESTIC_COMPETITION_ID					AS STRING) 		AS DOMESTIC_COMPETITION_ID,
			CAST(TOTAL_MARKET_VALUE 						AS INTEGER) 	AS TOTAL_MARKET_VALUE,
			CAST(SQUAD_SIZE									AS INTEGER) 	AS SQUAD_SIZE,
			CAST(AVERAGE_AGE 								AS DECIMAL) 	AS AVERAGE_AGE,
			CAST(FOREIGNERS_NUMBER							AS INTEGER) 	AS FOREIGNERS_NUMBER,
			CAST(FOREIGNERS_PERCENTAGE						AS DECIMAL) 	AS FOREIGNERS_PERCENTAGE,
			CAST(NATIONAL_TEAM_PLAYERS						AS INTEGER) 	AS NATIONAL_TEAM_PLAYERS,
			CAST(STADIUM_NAME								AS STRING) 		AS STADIUM_NAME,
			CAST(STADIUM_SEATS								AS INTEGER) 	AS STADIUM_SEATS,
			CAST(NET_TRANSFER_RECORD						AS STRING) 		AS NET_TRANSFER_RECORD,
			CAST(COACH_NAME									AS STRING) 		AS COACH_NAME,
			CAST(LAST_SEASON								AS INTEGER) 	AS LAST_SEASON,
			CAST(URL										AS STRING) 		AS URL,
			CURRENT_DATE() 													AS LoadDate,
			CAST('Transfermarkt'							AS STRING) 		AS RecordSource,
			CAST(MD5(CLUB_ID) 								AS CHAR(32)) 	AS ClubHashKey,
			MD5(UPPER(CONCAT(
                    TRIM(NVL(CLUB_CODE                      , '')), '||',
                    TRIM(NVL(NAME                      		, '')), '||',
                    TRIM(NVL(DOMESTIC_COMPETITION_ID       	, '')), '||',
                    TRIM(NVL(TOTAL_MARKET_VALUE         	, '')), '||',
                    TRIM(NVL(SQUAD_SIZE                     , '')), '||',
                    TRIM(NVL(AVERAGE_AGE                    , '')), '||',
                    TRIM(NVL(FOREIGNERS_NUMBER              , '')), '||',
                    TRIM(NVL(FOREIGNERS_PERCENTAGE          , '')), '||',
                    TRIM(NVL(NATIONAL_TEAM_PLAYERS          , '')), '||',
                    TRIM(NVL(STADIUM_NAME                 	, '')), '||',
                    TRIM(NVL(STADIUM_SEATS                  , '')), '||',
					TRIM(NVL(NET_TRANSFER_RECORD            , '')), '||',
					TRIM(NVL(COACH_NAME                     , '')), '||',
					TRIM(NVL(LAST_SEASON                    , '')), '||',
                    TRIM(NVL(URL                 			, ''))
			))) 															AS ClubHashDiff
FROM 		clubs