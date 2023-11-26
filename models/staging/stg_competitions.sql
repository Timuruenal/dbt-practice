WITH competitions AS (
			SELECT *
			FROM {{ source('transfermarkt', 'competitions')}}
)
SELECT 
			CAST(COMPETITION_ID 						AS STRING) 		AS COMPETITION_ID,
			CAST(COMPETITION_CODE 						AS STRING) 		AS COMPETITION_CODE,
			CAST(NAME 									AS STRING) 		AS NAME,
			CAST(SUB_TYPE 								AS STRING) 		AS SUB_TYPE,
			CAST(TYPE 									AS STRING) 		AS TYPE,
			CAST(COUNTRY_ID 							AS INTEGER) 	AS COUNTRY_ID,
			CAST(COUNTRY_NAME 							AS STRING) 		AS COUNTRY_NAME,
			CAST(DOMESTIC_LEAGUE_CODE 					AS STRING) 		AS DOMESTIC_LEAGUE_CODE,
			CAST(CONFEDERATION 							AS STRING) 		AS CONFEDERATION,
			CAST(URL 									AS STRING) 		AS URL,
			CURRENT_DATE() 												AS LoadDate,
			CAST('Transfermarkt'						AS STRING) 		AS RecordSource,
			CAST(MD5(COMPETITION_ID) 					AS CHAR(32)) 	AS CompetitionHashKey,
			MD5(UPPER(CONCAT(
                    TRIM(NVL(COMPETITION_CODE           , '')), '||',
                    TRIM(NVL(NAME                     	, '')), '||',
                    TRIM(NVL(SUB_TYPE                   , '')), '||',
                    TRIM(NVL(TYPE                       , '')), '||',
                    TRIM(NVL(COUNTRY_ID               	, '')), '||',
                    TRIM(NVL(COUNTRY_NAME               , '')), '||',
                    TRIM(NVL(DOMESTIC_LEAGUE_CODE       , '')), '||',
                    TRIM(NVL(CONFEDERATION            	, '')), '||',
                    TRIM(NVL(URL           				, ''))
			))) 														AS CompetitionHashDiff
FROM    	competitions