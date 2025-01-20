##data_cleaaaning

select * 
from world_layoffs.layoffs;

# double click on schema "world_layoffs" to select it
select *
from layoffs;

-- 1. Remove duplicates
-- 2. standardize the data
-- 3. Null or black values
-- 4. remove any columns, which aren't neccesary, if needed


#in the following, we create a table and performs actions on this new table so that we have a raw table available.
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1. remove duplicates
select *
from layoffs;

# we want to assign a row_number to each of row so that we can distinguish each one of them
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

# This CTE will give us all the duplicates 
with duplicate_cte as 
(
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

#Check if the rows in CTE are distinct, if yes then the partition by columns were good choice. If not, we need to partition by other columns
select *
from layoffs_staging
where company = 'Oda';
# we realize that our columns in partition by are not good choices. we do all the columns now 

##AGAIN
#we create the partition by again
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage, country, funds_raised_millions) as row_num
from layoffs_staging;

# This CTE will give us all the duplicates 
with duplicate_cte2 as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte2
where row_num > 1;

# create another table which has a column 'row_num'
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

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage, country, funds_raised_millions) as row_num
from layoffs_staging;

#see more than 1 entries
select *
from layoffs_staging2
where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

#delete the extra entries
delete
from layoffs_staging2
where row_num > 1;

#see again and the table will be empty
select *
from layoffs_staging2
where row_num > 1;



-- 2. standardizing data

#find all the blank spaces in the names of the companies and remove the extra space
select distinct(company), trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;
# take a look and you find e.g., crypto and crypto currency, they should be the same as they are same and we want them to appear under same name

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

#check location
select distinct location 
from layoffs_staging2
order by 1;

#check country
select distinct country
from layoffs_staging2
order by 1;

# found a period at the end of United states, we going to fix it
##could be done as we did before
#(update layoffs_staging2
#set country = 'United States'
#where country like 'United States%';)

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country);

# organize the data according to the time(do time series)
select `date`,
str_to_date(`date`, '%m/%d/%Y') as new_date
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select *
from layoffs_staging2;

#change the data type of date column
alter table layoffs_staging2
modify column `date` Date;

select *
from layoffs_staging2;

-- 3. null and black values

select *
from layoffs_staging2
where industry  is Null
or industry = '';

#we try to populate data e.g. Airbnb
select *
from layoffs_staging2
where company = 'Airbnb';

Update layoffs_staging2
set industry = Null
where industry = '';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company= t2.company
	and t1.location = t2.location
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company= t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
	where t1.industry is null
	and t2.industry is not null;

select *
from layoffs_staging2
where industry  is Null
or industry = '';

select *
from layoffs_staging2
where company like 'Bally%';
# Bally didnt have any other populated row so it didn't change. For other null values, we dont have enough data to populate the blanks

-- 4. remove any columns and rows we need to
select *
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is Null;
 
 #In the given context, these entries could be useless for us. So maybe we want to remove it
delete 
from layoffs_staging2
where total_laid_off is Null
and percentage_laid_off is Null;

select *
from layoffs_staging2;

#we dont need row_num anymore

alter table layoffs_staging2
drop column row_num;