DROP TABLE IF EXISTS layoffs;

CREATE TABLE layoffs
(
	company VARCHAR(255),
    location VARCHAR(255),
    industry VARCHAR(255),
    total_laid_off INT,
    percentage_laid_off FLOAT(5,2),
    date VARCHAR(255),
    stage VARCHAR(255),
    country VARCHAR(255),
    funds_raised_millions INT
);

LOAD DATA LOCAL INFILE '/Users/helloworld/Desktop/layoffs.csv'
INTO TABLE world_layoffs.layoffs
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- DATA CLEANING
SELECT *
FROM layoffs;

-- STAGING
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- REMOVE DUPLICATES
WITH depulicate_cte AS(
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM depulicate_cte
WHERE row_num > 1; 


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging2` (
  `company` varchar(255) DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `industry` varchar(255) DEFAULT NULL,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` decimal(5,2) DEFAULT NULL,
  `date` varchar(255) DEFAULT NULL,
  `stage` varchar(255) DEFAULT NULL,
  `country` varchar(255) DEFAULT NULL,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- STANDARDIZE DATE
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- POPULATE NULL AND BLANK VALUES 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry = ''
OR industry IS NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT company, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	USING (company)
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry != '');

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	USING (company)
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry != '');

-- REMOVE ANY COLUMNS
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL; -- 361 rows affected
    
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;