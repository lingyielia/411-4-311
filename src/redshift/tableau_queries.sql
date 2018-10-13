-- group by certain range of longitude, latitude for recent trend
SELECT ROUND(CAST("eventsnew"."longitude" AS DOUBLE PRECISION),CAST(2 AS INTEGER)) AS "calculation_1429822562764091433",
       ROUND(CAST("eventsnew"."latitude" AS DOUBLE PRECISION),CAST(2 AS INTEGER)) AS "calculation_1429822562764267562",
       COUNT(DISTINCT CAST("eventsnew"."created_date" AS DATE)) AS "temp(calculation_1429822561709133844)(1021017755)(0)",
       SUM(1) AS "temp(calculation_1429822561709133844)(3357548483)(0)"
FROM "public"."eventsnew" "eventsnew"
WHERE ((CAST("eventsnew"."created_date" AS DATE) < DATEADD(DAY,-1,CAST(CURRENT_DATE AS DATE))) AND
      (NOT (("eventsnew"."latitude" = 20.86125849849244) AND ("eventsnew"."longitude" = -23.92566793186856))) AND
      ("eventsnew"."agency" >= 'ACS') AND
      ("eventsnew"."agency" <= 'TLC'))
GROUP BY 1, 2;


-- counts of cases group by agency limit top 10
SELECT TOP 10 COUNT(DISTINCT CAST("eventsnew"."created_date" AS DATE)) AS "temp(calculation_1429822561709133844)(1021017755)(0)",
       SUM(1) AS "temp(calculation_1429822561709133844)(3357548483)(0)",
       "eventsnew"."agency" AS "agency"
FROM "public"."eventsnew" "eventsnew"
WHERE (CAST("eventsnew"."created_date" AS DATE) < DATEADD(DAY,-1,CAST(CURRENT_DATE AS DATE)))
GROUP BY 3
ORDER BY 2 DESC;


-- counts of cases resolved between 1 and 2 days group by agency
SELECT COUNT(DISTINCT CAST("eventsnew"."created_date" AS DATE)) AS "temp(calculation_1429822561709133844)(1021017755)(0)",
       SUM(1) AS "temp(calculation_1429822561709133844)(3357548483)(0)",
       "eventsnew"."agency" AS "agency"
FROM "public"."eventsnew" "eventsnew"
WHERE ((CAST("eventsnew"."created_date" AS DATE) < DATEADD(DAY,-1,CAST(CURRENT_DATE AS DATE))) AND
      (DATEDIFF(HOUR,CAST("eventsnew"."created_date" AS TIMESTAMP WITHOUT TIME ZONE),CAST("eventsnew"."closed_date" AS TIMESTAMP WITHOUT TIME ZONE)) > 24) AND
      (DATEDIFF(HOUR,CAST("eventsnew"."created_date" AS TIMESTAMP WITHOUT TIME ZONE),CAST("eventsnew"."closed_date" AS TIMESTAMP WITHOUT TIME ZONE)) <= 48))
GROUP BY 3;


-- average daily count
SELECT COUNT(DISTINCT CAST("eventsnew"."created_date" AS DATE)) AS "temp(calculation_1429822561709133844)(1021017755)(0)",
       SUM(1) AS "temp(calculation_1429822561709133844)(3357548483)(0)"
FROM "public"."eventsnew" "eventsnew"
WHERE ((CAST("eventsnew"."created_date" AS DATE) < DATEADD(DAY,-1,CAST(CURRENT_DATE AS DATE))) AND
      ("eventsnew"."agency" >= 'ACS') AND
      ("eventsnew"."agency" <= 'TLC'))
HAVING (COUNT(1) > 0)
