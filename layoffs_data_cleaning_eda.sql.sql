/* =====================================================
   PROJECT: SQL Data Cleaning & EDA – Global Layoffs
   DATABASE: MySQL
   ===================================================== */

-- ===============================
-- STEP 1: CREATE STAGING TABLE
-- ===============================

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;


-- ===============================
-- STEP 2: REMOVE DUPLICATES
-- ===============================

CREATE TABLE layoffs_staging2 AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off,
                        percentage_laid_off, `date`, stage, funds_raised_millions
           ORDER BY company
       ) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- ===============================
-- STEP 3: STANDARDIZE DATA
-- ===============================

-- Trim company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardize industry names
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Remove trailing dots from country
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert date column to DATE format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- ===============================
-- STEP 4: HANDLE NULL & BLANK VALUES
-- ===============================

-- Convert blanks to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate missing industry values using self join
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
   AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Remove records where both key metrics are NULL
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Drop helper column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- ===============================
-- STEP 5: EXPLORATORY DATA ANALYSIS (EDA)
-- ===============================

-- Max layoffs
SELECT MAX(total_laid_off) AS max_layoffs
FROM layoffs_staging2;

-- Companies with 100% layoffs
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Total layoffs by company
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company
ORDER BY total_layoffs DESC;

-- Layoffs date range
SELECT MIN(`date`) AS start_date, MAX(`date`) AS end_date
FROM layoffs_staging2;

-- Layoffs by industry
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_layoffs DESC;

-- Layoffs by country
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY total_layoffs DESC;

-- Layoffs by year
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY year
ORDER BY year DESC;

-- Monthly layoffs
SELECT DATE_FORMAT(`date`, '%Y-%m') AS month,
       SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY month
ORDER BY month;


-- ===============================
-- STEP 6: ROLLING TOTAL ANALYSIS
-- ===============================

WITH Monthly_Layoffs AS (
    SELECT DATE_FORMAT(`date`, '%Y-%m') AS month,
           SUM(total_laid_off) AS total_layoffs
    FROM layoffs_staging2
    GROUP BY month
)
SELECT month,
       total_layoffs,
       SUM(total_layoffs) OVER (ORDER BY month) AS rolling_total
FROM Monthly_Layoffs;


-- ===============================
-- STEP 7: TOP 5 COMPANIES PER YEAR
-- ===============================

WITH Company_Year AS (
    SELECT company,
           YEAR(`date`) AS year,
           SUM(total_laid_off) AS total_layoffs
    FROM layoffs_staging2
    GROUP BY company, year
),
Ranked_Companies AS (
    SELECT *,
           DENSE_RANK() OVER (PARTITION BY year ORDER BY total_layoffs DESC) AS ranking
    FROM Company_Year
)
SELECT *
FROM Ranked_Companies
WHERE ranking <= 5;
