/*Data cleaning
Steps followed for data cleaning

1. Remove Duplicate values/rows
2. Standardization of data
3. Dealing with null values
	-- Filling numeric null valued columns using statistical analysis
4. Remove unwanted columns
5. Adding of valuable new columns after cleaning 
*/

USE world_layoffs;

SELECT * FROM layoffs;

-- keeping raw_data table and creating a new table for cleaning
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging
ORDER BY 1;

/*
INSERT INTO layoffs_staging(company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions)
SELECT company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions FROM layoffs_staging
WHERE company = 'Atlassian';
*/

-- cte to find the duplicated rows
WITH find_duplicate_cte AS
(
-- using row_number()
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging)

SELECT * FROM layoffs_staging2
WHERE row_num > 1;

-- importing data from cte to a new table with row_num which cannot be used to filter in layoffs_staging
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2;

SELECT DISTINCT company
FROM layoffs_staging2
ORDER BY 1;

SELECT company, TRIM(company)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT company
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT `date` , STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- null values in laid_off and percentage_laid_off
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- remove meaningless columns
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- null values in industry
SELECT * FROM layoffs_staging2
WHERE industry IS NULL;

SELECT * FROM layoffs_staging;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';


-- joining two tables
SELECT tb1.industry,tb2.industry
FROM layoffs_staging2 tb1
JOIN layoffs_staging2 TB2
	ON tb1.company = tb2.company
WHERE (tb1.industry IS NULL OR tb1.industry = '')
AND tb2.industry <> '';

SELECT * FROM layoffs_staging2
WHERE company = "Bally's Interactive";

-- null values in funds_raised_millions
SELECT * FROM layoffs_staging2
WHERE funds_raised_millions IS NULL OR funds_raised_millions = '';

-- null values in stage
SELECT * FROM layoffs_staging2
WHERE stage IS NULL OR stage = '';

-- removed row_num column which is not needed anymore
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Further more analysis to make table data organised in manner.
-- Adding a new column to represent the total employees

ALTER TABLE layoffs_staging2
ADD COLUMN total_employees INT;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL 
AND percentage_laid_off IS NOT NULL;

-- Updating total_employees column with help of values in total_laid_off and percentage_laid_off
UPDATE layoffs_staging2
SET total_employees = (total_laid_off/(percentage_laid_off * 100)) * 100
WHERE total_laid_off IS NOT NULL 
AND percentage_laid_off IS NOT NULL;

-- Industry column null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

UPDATE layoffs_staging2
SET industry = 'Media'
WHERE company = "Bally's Interactive";

-- Instead of getting standard median value to fill null cells, taken median value based on industry type 
WITH median_total_laid_off (company,total_laid_off,industry,row_asc,row_desc)AS
(
SELECT company,total_laid_off,industry,
CAST(ROW_NUMBER() OVER(PARTITION BY industry ORDER BY total_laid_off) AS SIGNED) AS row_asc,
CAST(ROW_NUMBER() OVER(PARTITION BY industry ORDER BY total_laid_off DESC)AS SIGNED) AS row_desc
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
), final_median_value AS
(SELECT industry,ROUND(AVG(total_laid_off)) AS median
FROM median_total_laid_off
WHERE (row_asc - row_desc) IN (0,1,-1)
GROUP BY industry
)UPDATE layoffs_staging2 ls
JOIN final_median_value fm
ON fm.industry = ls.industry
SET ls.total_laid_off  = fm.median
WHERE ls.total_laid_off IS NULL;


-- percentage_laid_off median value by industry types
WITH median_percentage_laid_off (company,percentage_laid_off,industry,row_asc,row_desc)AS
(
SELECT company,percentage_laid_off,industry,
CAST(ROW_NUMBER() OVER(PARTITION BY industry ORDER BY percentage_laid_off) AS SIGNED) AS row_asc,
CAST(ROW_NUMBER() OVER(PARTITION BY industry ORDER BY percentage_laid_off DESC)AS SIGNED) AS row_desc
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL
), final_median_value AS
(
SELECT industry, AVG(percentage_laid_off) AS median
FROM median_percentage_laid_off
WHERE (row_asc - row_desc) IN (0,1,-1)
GROUP BY industry
)UPDATE layoffs_staging2 ls
JOIN final_median_value fm
ON fm.industry = ls.industry
SET ls.percentage_laid_off = fm.median
WHERE ls.percentage_laid_off IS NULL;

-- Getting columns that has only null values and not any not null valued columns
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
OR percentage_laid_off IS NULL;

SELECT AVG(total_laid_off)
FROM layoffs_staging2;

/* Filling those null value cells in both total_laid_off and percentage_laid_off columns but
Irrespective of industry type, taking all values to find out the mean!
*/
-- Mean value of total_laid_off
SELECT ROUND(AVG(total_laid_off))
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL;

-- Updated the mean value
UPDATE layoffs_staging2
SET total_laid_off = 328
WHERE total_laid_off IS NULL;

-- Mean value of percentage_laid_off
SELECT ROUND(AVG(percentage_laid_off),2)
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- Updated the mean value
UPDATE layoffs_staging2
SET percentage_laid_off = 0.19
WHERE percentage_laid_off IS NULL;

-- Updating total_employees column after statistical analysis
UPDATE layoffs_staging2
SET total_employees = (total_laid_off/(percentage_laid_off * 100)) * 100
WHERE total_employees IS NULL;

UPDATE layoffs_staging2
SET percentage_laid_off = ROUND(percentage_laid_off,2);

