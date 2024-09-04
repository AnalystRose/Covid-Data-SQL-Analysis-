-- confirming that the tables imported correctly
SELECT *
FROM coviddeaths
WHERE continent IS NOT NULL;

SELECT *
FROM covidvaccinations;

-- Selecting the columns we'll be using for analysis
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1,2;

-- converting the date to right, consistent format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM coviddeaths;
-- then updating the coviddeaths table
UPDATE coviddeaths
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Looking at Total Cases vs. Total deaths
-- shows likelihood of death if you contracted Covid,
SELECT continent, location, `date`, total_cases, total_deaths, (total_deaths/total_cases)*100 AS percentage_deaths
FROM coviddeaths
WHERE continent = 'Africa'
AND continent IS NOT NULL
ORDER BY 1,2;

-- Looking at Total Cases Vs. Population
-- Shows percentage of the population that got Covid
SELECT location, `date`, population, total_cases, (total_cases/population)*100 AS PercentagePopulationInfected
FROM coviddeaths
WHERE continent = 'Africa'
AND continent IS NOT NULL
ORDER BY 1,2;

-- ROLLING SUM COUNT
-- introducing the RollingSum for total_cases
SELECT location, `date`, population, total_cases,
SUM(total_cases) OVER (PARTITION BY location ORDER BY `date`)
AS RollingTotalCases
FROM coviddeaths
WHERE continent IS NOT NULL;

-- Looking at countries with the Highest Infection Rate compared to Population
SELECT location, population, MAX(total_cases) AS HighestInfectionCount, MAX((total_cases/population)*100) AS PercentagePopulationInfected
FROM coviddeaths
GROUP BY location, population
ORDER BY 4 DESC;

-- Looking at countries with the Highest Death Count per population
SELECT location, MAX(total_deaths) AS HighestDeathCount
FROM coviddeaths
GROUP BY location
ORDER BY 2 desc;

-- while this query runs, there is a problem with the data type for total deaths, hence introducing the CAST fxn
SELECT location, MAX(cast(total_deaths as signed int)) AS HighestDeathCount
FROM coviddeaths
GROUP BY location
ORDER BY 2 desc;


-- BREAKING THINGS DOWN BY CONTINENT
-- Showing continents with the highest death count
SELECT continent, MAX(cast(total_deaths as signed int)) AS TotalDeathCount
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1
ORDER BY 2 desc;

-- Looking at Total Deaths vs Population
SELECT location, population, MAX(cast(total_deaths as signed int)) AS TotalDeathCount, MAX(cast(total_deaths as signed int))/population*100 AS PercentageDeathCount
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY 4 DESC;


-- GLOBAL NUMBERS
-- here, we eliminate anything that segregates in terms of location/continent
-- then, introduce aggregate function to everything else, leaving only what you want to group by

SELECT `date`, SUM(new_cases) AS TotalCases, SUM(cast(new_deaths as signed int)) AS TotalDeaths, SUM(cast(new_deaths as signed int))/SUM(new_cases)*100 AS DeathPercentage
FROM coviddeaths
-- WHERE continent = 'Africa'
GROUP BY `date`
ORDER BY 1, 2;

-- Looking at a clearer global perspective
SELECT SUM(new_cases) AS TotalCases, SUM(cast(new_deaths as signed int)) AS TotalDeaths, SUM(cast(new_deaths as signed int))/SUM(new_cases)*100 AS DeathPercentage
FROM coviddeaths 
-- WHERE continent = 'Africa'
-- GROUP BY `date`

-- Updating correct date format for Vaccinations table
SELECT `date`
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM covidvaccinations;

UPDATE covidvaccinations
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- JOINING Deaths and Vaccinations tables and introducing aliases 
SELECT *
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`;
    
-- Looking at the Total Population vs Vaccinations
-- Specifying table from which column we use in the SELECT clause 
-- runs without the ORDER BY fxn
SELECT dea.continent, dea.location, dea.`date`, dea.population, vac.new_vaccinations
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`
WHERE dea.continent IS NOT NULL     
    ORDER BY 1,2,3;

-- Rolling SUm Count on New Vaccinations 
-- Partitioning by location such that once a new location is reached, the aggregate starts over 
SELECT dea.continent, dea.location, dea.`date`, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as signed int)) OVER (PARTITION BY dea.location) AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`;
    
-- because, we partitioned by location only, the output gives, gives one total for the entire location, not a rolling sum, 
-- to change that, include date in the PARTITION
SELECT dea.continent, dea.location, dea.`date`, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as signed int)) OVER (PARTITION BY dea.location ORDER BY dea.`date`) 
AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`;

-- Total population vs Vaccinations-- Using the totalRollingPeopleVaccinated per location vs the country's total populus
-- however, we cannot directly use the Rolling column for a new operation as its new [just created]
-- INTRODUCING a CTE
-- NOTE that the number of columns in a CTE must match those in the SELECT statement
-- NOTE that the ORDER BY clause cant be used in a CTE
WITH PopvsVac (continent, location, `date`, population, new_vaccinantions, RollingPeopleVaccinated)
as
(
SELECT dea.continent, dea.location, dea.`date`, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as signed int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) 
AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`
)
SELECT*
FROM PopvsVac;

-- Looking at RollingPeopleVaccinated percentage
WITH PopvsVac (continent, location, `date`, population, new_vaccinantions, RollingPeopleVaccinated)
as
(
SELECT dea.continent, dea.location, dea.`date`, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as signed int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) 
AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`
)
SELECT*, (RollingPeopleVaccinated/population)*100 
FROM PopvsVac;

-- USING TEMP TABLE***
-- adding the DROP TABLE clause allows for making changes
DROP TABLE IF EXISTS PercentagePopulationVaccinated
CREATE TABLE PercentagePopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),	
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)
INSERT INTO PercentagePopulationVaccinated
SELECT dea.continent, dea.location, dea.`date`, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as signed int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) 
AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`
)
SELECT*, (RollingPeopleVaccinated/population)*100 
FROM PercentagePopulationVaccinated;

-- Creating VIEW to store data for later visualizations
-- ORDER BY clause not used in views
CREATE VIEW PercentagePopulationVaccinated AS
SELECT dea.continent, dea.location, dea.`date`, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as signed int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) 
AS RollingPeopleVaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
    AND dea.`date` = vac.`date`
WHERE dea.continent IS NOT NULL;

-- once the view is created, it serves almost like a table, allowing to query off of    
SELECT *
FROM PercentagePopulationVaccinated;













