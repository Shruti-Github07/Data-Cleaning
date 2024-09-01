-- SQL Project - Data Cleaning

-- Querying all records from the original layoffs table
SELECT * 
FROM world_layoffs.layoffs;

-- Creating a staging table with the same structure as the original
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

-- Copying all data into the staging table
INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- 1. Removing Duplicates
-- Let's start by identifying any duplicate rows

SELECT *
FROM world_layoffs.layoffs_staging;

-- Check for duplicates using ROW_NUMBER
SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Isolating duplicate rows by assigning row numbers and filtering
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Verifying specific entries to ensure accuracy before deletion
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';

-- Here, we identify actual duplicates that should be deleted
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Filter and delete rows where row number is greater than 1

WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE;

-- Another approach to delete duplicates using a CTE

WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- Alternatively, add a row number column, then delete rows where the row number exceeds 1, and finally drop the column

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

-- Display the staging table with the new row number column
SELECT *
FROM world_layoffs.layoffs_staging;

-- Creating a new staging table with the added row_num column
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

-- Inserting data into the new staging table with row numbers assigned
INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Deleting rows with row_num greater than or equal to 2
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- 2. Standardizing Data

-- Checking the data in the new staging table
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Examining the distinct values in the 'industry' column to identify issues
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Finding rows with null or empty 'industry' values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Manually checking specific companies to understand missing data
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- If the industry value is missing, we can populate it based on existing data for the same company

-- Convert blank entries to NULL for easier manipulation
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Verify that all blanks are now NULLs
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Populate NULL industry values with non-null values from the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Checking for any remaining nulls after the update
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Standardizing 'Crypto' industry values to maintain consistency
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Confirm that the industry standardization is complete
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Checking and fixing variations in the 'country' field, such as trailing periods
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Removing trailing periods from country names
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Verifying that country names are standardized
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Fixing the date column format by converting it to a date type
SELECT *
FROM world_layoffs.layoffs_staging2;

-- Convert string dates to proper date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the data type of the date column to DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Checking the final data structure
SELECT *
FROM world_layoffs.layoffs_staging2;

-- 3. Handling Null Values

-- Deciding not to alter null values in key numeric fields for ease of analysis
-- Null values in total_laid_off, percentage_laid_off, and funds_raised_millions are acceptable for now

-- 4. Removing Unnecessary Data

-- Identifying rows with missing values in key fields
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

-- Deleting rows with missing data in both total_laid_off and percentage_laid_off
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Final check of the cleaned dataset
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Dropping the row_num column now that it is no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final view of the staging table after cleaning
SELECT * 
FROM world_layoffs.layoffs_staging2;
