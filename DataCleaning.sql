-- Data Cleaning Porject

/** All Steps:
	1- Remove Duplicates values
    2- Standardizing Data
    3- Null Values or Blank Values
**/

USE world_layoffs;


SELECT *
FROM
	world_layoffs.layoffs
;


CREATE TABLE layoff_staging
LIKE layoffs
;


SELECT *
FROM
	world_layoffs.layoff_staging
;


INSERT INTO layoff_staging
SELECT *
FROM
	world_layoffs.layoffs
;


SELECT *
FROM
	world_layoffs.layoff_staging
;


SELECT *,
	ROW_NUMBER() OVER(ORDER BY funds_raised_millions DESC)
FROM
	world_layoffs.layoff_staging
;


SELECT *,
	RANK() OVER(ORDER BY funds_raised_millions)
FROM
	world_layoffs.layoff_staging
;



SELECT *,
	DENSE_RANK() OVER(ORDER BY funds_raised_millions)
FROM
	world_layoffs.layoff_staging
;


SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
    percentage_laid_off, `date` ,stage, country, funds_raised_millions) AS row_num
FROM
	world_layoffs.layoff_staging
;


WITH duplicate_cte 
AS (
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
    percentage_laid_off, `date` ,stage, country, funds_raised_millions) AS row_num
FROM
	world_layoffs.layoff_staging)
SELECT *
FROM
	duplicate_cte 
WHERE
	row_num > 1
;


SELECT *
FROM
	world_layoffs.layoff_staging
WHERE
	company = 'Casper'
;


SELECT *
FROM
	world_layoffs.layoff_staging
WHERE
	company = 'Cazoo'
;


SELECT *
FROM
	world_layoffs.layoff_staging
WHERE
	company = 'Bybit'
;



SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off,
    percentage_laid_off, `date` ,stage, country, funds_raised_millions) AS row_num
FROM
	layoff_staging
WHERE
	company= 'Bybit'
;



SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    `date`, stage, country, total_laid_off, percentage_laid_off,
    funds_raised_millions) AS row_num
FROM
	layoff_staging
;

WITH duplicate_value AS
(
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    `date`, stage, country, total_laid_off, percentage_laid_off,
    funds_raised_millions) AS row_num
FROM
	layoff_staging)
SELECT *
FROM
	duplicate_value
WHERE
	row_num >1
;


SELECT *
FROM
	world_layoffs.layoff_staging
WHERE
	company = 'Yahoo'
;



-- for delete duplicate values in sql server
WITH duplicate_value AS
(
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    `date`, stage, country, total_laid_off, percentage_laid_off,
    funds_raised_millions) AS row_num
FROM
	layoff_staging)
DELETE
FROM
	duplicate_value
WHERE
	row_num >1
;


CREATE TABLE `layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_number` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs2
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    `date`, stage, country, total_laid_off, percentage_laid_off,
    funds_raised_millions) AS row_num
FROM
	layoff_staging
;


SELECT *
FROM
	world_layoffs.layoffs2
;


-- change column name row_number because this is a key word
ALTER TABLE layoffs2
CHANGE `row_number` row_num INT
;


SELECT *
FROM
	world_layoffs.layoffs2
WHERE 	
	row_num > 1
;     

-- Remove Duplicate Values
DELETE
FROM
	world_layoffs.layoffs2
WHERE
	row_num > 1
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	row_num > 1
;


SELECT *
FROM
	world_layoffs.layoffs2
;


-- Standardizing Data

SELECT 
	DISTINCT(company),
    TRIM(company),
    CONCAT(UCASE(SUBSTRING(`company`, 1, 1)), LOWER(SUBSTRING(`company`, 2)))
FROM
	world_layoffs.layoffs2
;

SELECT 
	SUBSTRING('HELLO',1,1)
;

SELECT 
	CONCAT(UPPER(SUBSTRING('E Inc',1,1)) ,LOWER(SUBSTRING('E Inc',2)));



SELECT 
	DISTINCT(company),
    TRIM(company),
    CONCAT(UPPER(SUBSTRING(company, 1,1)), LOWER(SUBSTRING(company,2)))
FROM
	world_layoffs.layoffs2
;


UPDATE world_layoffs.layoffs2
SET company = TRIM(company)
;

SELECT *
FROM
	world_layoffs.layoffs2
;


UPDATE world_layoffs.layoffs2
SET company = CONCAT(UPPER(SUBSTRING(company, 1,1)), LOWER(SUBSTRING(company,2)))
;


SELECT *
FROM
	world_layoffs.layoffs2
;


SELECT 
	DISTINCT(location),
    TRIM(location)
FROM
	world_layoffs.layoffs2
;

UPDATE world_layoffs.layoffs2
SET location = TRIM(location)
;


UPDATE world_layoffs.layoffs2
SET location = CONCAT(UPPER(SUBSTRING(location,1,1)), LOWER(SUBSTRING(location,2)))
;


SELECT *
FROM
	world_layoffs.layoffs2
;


SELECT 
	DISTINCT(industry)
FROM
	world_layoffs.layoffs2
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	industry LIKE 'Crypto%'
;

UPDATE world_layoffs.layoffs2
SET industry = 'Crypto'
WHERE 
	industry LIKE 'Crypto%'
;


SELECT 
	DISTINCT(industry)
FROM
	world_layoffs.layoffs2
WHERE
	industry LIKE 'Crypto%'
;


UPDATE world_layoffs.layoffs2
SET industry = TRIM(industry)
;


SELECT 
	DISTINCT(location),
    CONCAT(UPPER(SUBSTRING(location,1,1)), LOWER(SUBSTRING(location,2)))
FROM
	world_layoffs.layoffs2
;

UPDATE world_layoffs.layoffs2
SET location = CONCAT(UPPER(SUBSTRING(location,1,1)), LOWER(SUBSTRING(location,2)))
;


SELECT *
FROM
	world_layoffs.layoffs2
;

SELECT 
	DISTINCT(country)
FROM
	world_layoffs.layoffs2
;


SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	country LIKE 'United States%'
ORDER BY
	1
;


SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	country LIKE '%.'
;


SELECT 
	TRIM(TRAILING '.' FROM country)
FROM
	world_layoffs.layoffs2
WHERE
	country LIKE '%.'
;

UPDATE world_layoffs.layoffs2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;


UPDATE world_layoffs.layoffs2
SET country = TRIM(country)
;


SELECT *
FROM
	world_layoffs.layoffs2
;


SELECT 
	DISTINCT(stage)
FROM
	world_layoffs.layoffs2
;
    

UPDATE world_layoffs.layoffs2
SET stage = TRIM(stage)
;    
    

SELECT *
FROM
	world_layoffs.layoffs2
;


SELECT 
	`date`,
    STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
	world_layoffs.layoffs2
;


UPDATE 	world_layoffs.layoffs2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%y')
;

ALTER TABLE layoffs2
MODIFY COLUMN `date` DATE;


UPDATE layoffs2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE STR_TO_DATE(`date`, '%m/%d/%Y') IS NOT NULL;
    
ALTER TABLE layoffs2
MODIFY COLUMN `date` DATE;

SELECT *
FROM
	world_layoffs.layoffs2
;


-- check blank values and change them to Null

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company IS NULL
;

-- company havent null

SELECT *
FROM
	world_layoffs.layoffs2
WHERE location IS NULL
;

-- location havent null

SELECT *
FROM
	world_layoffs.layoffs2
WHERE 
	industry IS NULL
;



SELECT *
FROM
	world_layoffs.layoffs2
WHERE 
	industry = ''
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company = 'Airbnb'
;

-- Industry have blank values

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company = ''
;

-- company havent blank values

-- change industry blank values to null
UPDATE world_layoffs.layoffs2
SET industry = NULL 
WHERE
	industry = ''
;

SELECT *
FROM
	world_layoffs.layoffs2 AS t1
INNER JOIN
	world_layoffs.layoffs2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL
;


UPDATE world_layoffs.layoffs2 AS t1
JOIN  world_layoffs.layoffs2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company = 'Airbnb'
;


SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	industry IS NULL
;


SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company = "Bally's interactive"
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	total_laid_off IS NULL
;


SELECT *
FROM
	world_layoffs.layoffs2 AS t1
INNER JOIN
	world_layoffs.layoffs2 t2
    ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.`date` = t2.`date`
WHERE t1.total_laid_off IS NULL
AND t2.total_laid_off IS NOT NULL
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company = 'Bybit'
;

UPDATE world_layoffs.layoffs2 AS t1
INNER JOIN world_layoffs.layoffs2 AS t2
ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.`date` = t2.`date`
SET t1.total_laid_off = t2.total_laid_off
WHERE t1.total_laid_off IS NULL
AND t2.total_laid_off IS NOT NULL
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE 
	total_laid_off IS NULL
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	percentage_laid_off IS NULL
;


SELECT *
FROM
	world_layoffs.layoffs2 AS t1
INNER JOIN
	world_layoffs.layoffs2 AS t2
	ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.`date` = t2.`date`
WHERE
	t1.percentage_laid_off IS NULL
AND t2.percentage_laid_off IS NOT NULL
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company = 'Bybit'
;

SELECT *
FROM
	world_layoffs.layoffs2 AS t1
INNER JOIN
	world_layoffs.layoffs2 AS t2
    ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.`date` = t2.`date`
WHERE
	t1.percentage_laid_off IS NULL
AND
	t2.percentage_laid_off IS NOT NULL
;


UPDATE world_layoffs.layoffs2 AS t1
INNER JOIN world_layoffs.layoffs2 AS t2
 ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.`date` = t2.`date`
SET t1.percentage_laid_off = t2.percentage_laid_off
WHERE
	t1.percentage_laid_off IS NULL
AND
	t2.percentage_laid_off IS NOT NULL
;

SELECT *
FROM
	world_layoffs.layoffs2
;


DELETE FROM world_layoffs.layoffs2
WHERE
	total_laid_off IS NULL AND percentage_laid_off IS NULL
;


SELECT *
FROM
	world_layoffs.layoffs2
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	`date` IS NULL
;

SELECT *
FROM
	world_layoffs.layoffs2 AS t1
INNER JOIN
	world_layoffs.layoffs2 AS t2
	ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.`date` = t2.`date`
WHERE 
	t1.`date` IS NULL
AND t2.`date` IS NOT NULL
;


SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	stage IS NULL
;

SELECT *
FROM
	world_layoffs.layoffs2 AS t1
INNER JOIN
	world_layoffs.layoffs2 AS t2
	ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.`date` = t2.`date`
WHERE 
	t1.stage IS NULL
AND t2.stage IS NOT NULL
;

SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	country IS NULL
;


SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	funds_raised_millions IS NULL
;

SELECT *
FROM
	world_layoffs.layoffs2 AS t1
INNER JOIN
	world_layoffs.layoffs2 AS t2
	ON t1.company = t2.company
    AND t1.industry = t2.industry
    AND t1.funds_raised_millions = t2.funds_raised_millions
WHERE 
	t1.stage IS NULL
AND t2.stage IS NOT NULL
;

SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    total_laid_off, percentage_laid_off, `date`, stage,
    country, funds_raised_millions)
FROM
	world_layoffs.layoffs2
	
;

WITH ctes_duplicates AS
(
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    total_laid_off, percentage_laid_off, `date`, stage,
    country, funds_raised_millions) AS rw_num2
FROM
	world_layoffs.layoffs2
)
SELECT *
FROM
	ctes_duplicates
WHERE
	rw_num2 >1
;
-- 1 duplicate value 
SELECT *
FROM
	world_layoffs.layoffs2
WHERE
	company = 'Bybit'
;

CREATE TABLE `layoffs3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL,
  `rw_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs3
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    total_laid_off, percentage_laid_off, `date`, stage,
    country, funds_raised_millions) AS rw_num2
FROM
	world_layoffs.layoffs2
;


SELECT *
FROM
	world_layoffs.layoffs3
;

SELECT *
FROM
	world_layoffs.layoffs3
WHERE
	rw_num > 1
;

DELETE FROM layoffs3
WHERE
	rw_num > 1
;

ALTER TABLE layoffs3
DROP COLUMN rw_num
;

ALTER TABLE layoffs3
DROP COLUMN row_num
;

SELECT *
FROM
	world_layoffs.layoffs3
;