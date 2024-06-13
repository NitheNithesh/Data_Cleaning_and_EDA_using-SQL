-- Exploratory Data Analysis

USE world_layoffs;

-- Sum of Total employees
SELECT SUM(total_employees) 
FROM layoffs_staging2;

-- Sum of Total laid off 
SELECT SUM(total_laid_off)
FROM layoffs_staging2;

-- total_laid_off by company
SELECT company,MAX(total_laid_off) AS Max_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- total _laid_off by industry
SELECT industry ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- total layoffs by date
SELECT `date` ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1;

-- total layoffs by MONTH 
SELECT MONTH(`date`) ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY MONTH(`date`)
ORDER BY 1;

SELECT SUM(total_laid_off)
FROM layoffs_staging2;

-- rolling sum total of layoffs
WITH Rolling_Total AS
(
SELECT `DATE`,SUM(total_laid_off) AS total_laid
FROM layoffs_staging2
GROUP BY `DATE`
ORDER BY 1
)
SELECT `date`,total_laid,
SUM(total_laid) OVER(ORDER BY `DATE`) AS Rolling_Date_Total
FROM Rolling_Total;


-- Ranking based on company by dates
WITH Company_Dates(Company,`Month`,total_laid_off) AS
(
SELECT company,SUBSTRING(`date`,6,2),SUM(total_laid_off)
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY company,SUBSTRING(`date`,6,2)
ORDER BY 2
),Company_Datewise_Ranks AS
(SELECT *,DENSE_RANK() OVER(PARTITION BY `Month` ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Dates
)
SELECT * FROM
Company_Datewise_Ranks
WHERE Ranking <= 3;

SELECT * 
FROM layoffs_staging2;

SELECT COUNT(DISTINCT(location))
FROM layoffs_staging2;

SELECT COUNT(DISTINCT(country))
FROM layoffs_staging2;


/* Top 3 companies with highest number of employees.
Considered ranking based on total_employees & funds_raised_millions column if total_employees
of two companies is same.
*/
WITH top3_emp_companies(company,Employees,Ranking) AS
(
SELECT company, SUM(total_employees) AS Employees,
DENSE_RANK() OVER(ORDER BY SUM(total_employees) DESC,funds_raised_millions DESC) AS Ranking
FROM layoffs_staging2
GROUP BY company,funds_raised_millions
)SELECT * 
FROM top3_emp_companies
WHERE Ranking <=3;

/*
Top 10 companies with low percentage laid off and also with high total_employees
i.e Big companies with more employees and less percentage layoffs
*/
WITH top_10_less_layoff_companies AS
(
SELECT company, location,industry,SUM(total_laid_off) AS total_layoffs, 
SUM(percentage_laid_off) AS percentage_layoffs,country,SUM(total_employees) AS total_employees,
DENSE_RANK() OVER(ORDER BY SUM(total_employees)DESC,SUM(percentage_laid_off)) AS Ranking
FROM layoffs_staging2
GROUP BY company, location,industry,country
)SELECT * 
FROM top_10_less_layoff_companies
WHERE Ranking <= 10;

/*
TOP 2 companies in each industry with less number of layoffs
Here I classified industries into three types based on total employees
1. Small - 0 to 1000 employees
2. Medium - 1001 to 5000 employees
3. Large - 5001 to 20000 employees
*/
WITH less_layoffs(company,industry,Total_layoff,percent_layoff,total_emp) AS
(
SELECT company,industry,SUM(total_laid_off) AS Total_layoff,
SUM(percentage_laid_off) AS percent_layoff,SUM(total_employees) AS total_emp
FROM layoffs_staging2
GROUP BY company,industry
),ranking_cte AS
(SELECT *,
DENSE_RANK() OVER(PARTITION BY industry ORDER BY Total_layoff,percent_layoff) AS Ranking
FROM less_layoffs
WHERE total_emp > 5000
)SELECT * FROM ranking_cte
WHERE Ranking <=2;
