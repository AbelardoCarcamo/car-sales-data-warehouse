CREATE TABLE StagingCarSales (
    year INT NULL,
    make VARCHAR(50) NULL,
    model VARCHAR(100) NULL,
    trim VARCHAR(100) NULL,
    body VARCHAR(50) NULL,
    transmission VARCHAR(50) NULL,
    vin VARCHAR(50) NULL,
    state CHAR(2) NULL,
    condition INT NULL,
    odometer INT NULL,
    color VARCHAR(50) NULL,
    interior VARCHAR(50) NULL,
    seller VARCHAR(200) NULL,
    mmr FLOAT NULL,
    sellingprice FLOAT NULL,
    saledate VARCHAR(100) NULL
);

SELECT COUNT(*) AS total_rows
FROM StagingCarSales;

SELECT TOP 10 *
FROM StagingCarSales;
USE CarSalesDW;

SELECT
	SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS years_nulls,
	SUM(CASE WHEN make IS NULL THEN 1 ELSE 0 END) AS makes_nulls,
	SUM(CASE WHEN sellingprice IS NULL THEN 1 ELSE 0 END) AS sellingprice_nulls,
	SUM(CASE WHEN model IS NOT NULL THEN 1 ELSE 0 END) AS models_nulls,
	SUM(CASE WHEN saledate IS NULL THEN 1 ELSE 0 END) AS saledate_null
FROM StagingCarSales;

SELECT COUNT(*)
FROM StagingCarSales
WHERE make IS NULL
  AND model IS NOT NULL;

SELECT model,
       COUNT(DISTINCT make) AS marcas_distintas
FROM StagingCarSales
WHERE make IS NOT NULL
GROUP BY model
HAVING COUNT(DISTINCT make) > 1
ORDER BY marcas_distintas DESC;

SELECT model,
       MAX(make) AS make
FROM StagingCarSales
WHERE make IS NOT NULL
GROUP BY model
HAVING COUNT(DISTINCT make) = 1;

UPDATE s
SET make = ref.make
FROM StagingCarSales s
JOIN (
    SELECT model, MAX(make) AS make
    FROM StagingCarSales
    WHERE make IS NOT NULL
    GROUP BY model
    HAVING COUNT(DISTINCT make) = 1
) ref
  ON s.model = ref.model
WHERE s.make IS NULL;

SELECT COUNT(*) AS make_null_restantes
FROM StagingCarSales
WHERE make IS NULL;


SELECT COUNT(*) AS modelos_ambiguos
FROM (
    SELECT model
    FROM StagingCarSales
    WHERE make IS NOT NULL
    GROUP BY model
    HAVING COUNT(DISTINCT make) > 1
) t;

SELECT
    model,
    MAX(make) AS make
INTO ModelMakeMap
FROM StagingCarSales
WHERE make IS NOT NULL
GROUP BY model
HAVING COUNT(DISTINCT make) = 1;

SELECT COUNT(*) AS make_a_rellenar
FROM StagingCarSales s
JOIN ModelMakeMap m
  ON s.model = m.model
WHERE s.make IS NULL;

SELECT COUNT(*)
FROM StagingCarSales s
JOIN ModelMakeMap m
  ON s.model = m.model
WHERE s.make IS NULL;

SELECT
    COUNT(*) AS total_make_null,
    COUNT(model) AS model_not_null
FROM StagingCarSales
WHERE make IS NULL;

DELETE FROM StagingCarSales
WHERE make IS NULL;

SELECT COUNT(*) 
FROM StagingCarSales
WHERE make IS NULL;

ALTER TABLE StagingCarSales
ALTER COLUMN make VARCHAR(100) NOT NULL;



