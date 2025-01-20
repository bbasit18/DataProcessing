## Exploratory Data analysis
-- the table
select *
from layoffs_staging2;

-- Max lay offs
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- everyone was laid off
select *
from layoffs_staging2
where percentage_laid_off = 1
order by date desc;

-- max to min lay off
select *
from layoffs_staging2
order by 4 desc;

-- total number of layoffs in health
select sum(total_laid_off)
from layoffs_staging2
where industry = 'healthcare';

-- total number of layoff in different industries
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc; 

-- total number of layoff in different countries
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc; 

-- total number of layoff in each year
select year(date), SUM(total_laid_off)
from layoffs_staging2
group by year(date)
order by 1 desc;

-- stage of the comapny
select stage, SUM(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- Time window for the data
select min(date), max(date)
from layoffs_staging2;

-- per month lay_offs
select substring(date,1,7) as month, sum(total_laid_off)
from layoffs_staging2
group by month
order by 1 desc;

-- progration of layoffs(rolling total) using CTE
with rolling_total as (
select substring(date,1,7) as months, SUM(total_laid_off) AS total_laid_offs
from layoffs_staging2
where substring(date,1,7) is not NULL
group by months
order by months asc
)
select months, total_laid_offs, 
sum(total_laid_offs) over (order by months) as Rolling_total
from rolling_total;

-- progration of layoffs(rolling total) without using CTE
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- running total of the companies
SELECT company, SUM(total_laid_off)
from layoffs_staging2
where total_laid_off is not NULL
group by company
order by 2 desc; 

-- Top 5 companies that did layoffs per year
with Company_year as
(
select company, Year(date) as years , SUM(total_laid_off) as total_laid_offs
from layoffs_staging2
group by company, Year(date)
), Company_year_rank as
(select * ,  dense_rank() over (partition by years order by total_laid_offs desc) as ranking
from Company_year
where years is not NULL
)
select *
from Company_year_rank
where ranking <= 5;

