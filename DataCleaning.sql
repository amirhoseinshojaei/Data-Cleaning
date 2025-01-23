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


