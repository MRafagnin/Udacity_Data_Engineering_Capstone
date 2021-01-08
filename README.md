# Udacity - Data Engineering Capstone Project

## Objective
> Build ETL pipeline on specific datasets to create an analytics database so to find patterns through data analysis.
> The purpose is to find patterns within the data around Immigration. One such case is "Are people migrating from colder to hotter weathers or vice-versa?".

## Scope & Steps taken

> Create analytics database through below steps:

- Spark loads data into dataframes;
- Data analysis of I94 immigration dataset - identify missing values and clean data;
- Data analysis of Demographics dataset - identify missing values and clean data;
- Data analysis of Global Land Temperatures - identify missing values clean data;
- Apply data cleaning functions on all datasets;
- Create fact table
- Creates dimension tables;
- Creates immigration_arrivals dimension table;
- Creates country dimension table;
- Create demographics dimension table;

Data is read and staged from user repository using Spark.


### Other possible Scenarios

- Data was increased by 100x: Increase the number of nodes in cluster
- Pipelines were run on a daily basis by 7am: Airflow would be implemented to handle pipelines
- Database needed to be accessed by 100+ people: Amazon Redshift would be implemented for analytics

## Choice of Technologies

- Spark - Ability to handle multiple file formats with large amounts of data.

- Updating of Data - Monthly (I94 immigration data is updated monthly)


## Technologies
Project is created with:
- Python 3
- AWS - Amazon S3
- Spark



## Files/Folder in Repository

- `etl.py` - reads data from S3, processes that data using Spark, and writes them back to S3

- `config.cfg` - AWS credentials (fill accordantly)

- `clean_functions.py` - contain functions for cleaning the data

- `create_functions.py` - contain functions that create the tables



## Datasets

### I94 Immigration Data
- This data comes from the US National Tourism and Trade Office - [here](https://travel.trade.gov/research/reports/i94/historical/2016.html)


### World Temperature Data
- This dataset came from Kaggle - [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

### U.S. City Demographic Data
- This data comes from OpenSoft - [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

### Airport Code Table
- This is a simple table of airport codes and corresponding cities - [here](https://datahub.io/core/airport-codes#data)


## Database Schema & Data Model

#### Fact Table:
        
##### - immigration_fact

- The data comes from the immigration dataset and contains keys that links to the dimension tables.

| Name | Description
| :-----: | :-: 
| record_id      | Unique record ID
| residence_code | 3 digit code for immigrant country of residence
| visa_type_key  | A numerical key that links to the visa_type dimension table
| state_code	 | US state of arrival
| i94yr	         | 4 digit year
| i94mon	 | Numeric month
| i94port	 | Port of admission
| arrdate	 | Arrival Date in the USA
| i94mode	 | Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)
| i94addr	 | USA State of arrival
| depdate	 | Departure Date from the USA
| i94bir	 | Age of Respondent in Years
| i94visa	 | Visa codes collapsed into three categories
| count	         | Field used for summary statistics
| dtadfile	 | Character Date Field - Date added to I-94 Files
| visapost	 | Department of State where where Visa was issued
| occup	         | Occupation that will be performed in U.S
| entdepa	 | Arrival Flag - admitted or paroled into the U.S.
| entdepd	 | Departure Flag - Departed, lost I-94 or is deceased
| entdepu	 | Update Flag - Either apprehended, overstayed, adjusted to perm residence
| matflag	 | Match flag - Match of arrival and departure records
| biryear	 | 4 digit year of birth
| dtaddto	 | Character Date Field - Date to which admitted to U.S. (allowed to stay until)
| gender	 | Non-immigrant sex
                    
                    
#### Dimension Tables:


##### - immigration_arrivals

- The data also comes from the immigration dataset and contains a key that links to the fact table above.

| Name | Description
| :-----: | :-: 
| id              | Unique id
| arrdate         | Arrival Date
| arrival_year    | Arrival Year
| arrival_month   | Arrival MonthS
| arrival_day     | Arrival Day
| arrival_week    | Arrival Week
| arrival_weekday | Arrival WeekDay
            
##### - demographics

- The data comes from the demographics dataset and links to the fact table via US state.
- It can provides insights on migration and population. It can help answer questions such as: Which states attract more visitors?; which can be valuable for decision making around tourism.
- The table allows for insights into migration based on demographics at a granular level.

| Name | Description
| :-----: | :-: 
| id                     | Record id
| state_code             | US state code
| City                   | City Name
| State                  | State where city is located
| Median Age             | Median age of the population
| Male Population        | Count of male population
| Female Population      | Count of female population
| Total Population       | Count of total population
| Number of Veterans     | Count of total Veterans
| Foreign born           | Count of residents not born in the city
| Average Household Size | Average city household size
| Race                   | Respondent race
| Count                  | Count of city's individual per race
            
##### - country

- The data comes from the temperature and immigration dataset;
- It can provide insights into immigration patterns against temperatures on a per city level.
- The combination of these datasets allows for the study of correlations between temperature and immigration patterns.

| Name | Description
| :-----: | :-: 
| country_code        | Unique country code
| country_name        | Name of country
| average_temperature | Average temperature of country

##### - visatype

- The data also comes from the immigration dataset and links to the fact table via the visa_key.

| Name | Description
| :-----: | :-: 
visa_type_id | Unique id for each visa issued
visa_type    | Name of visa



## Data Pipeline

- Load datasets
- Clean Immigration data and create Spark dataframe by month
- Create visa_type table
- Create arrival table
- Process temperatures data
- Create country table
- Create immigration fact table
- Load demographics data
- Clean demographics data
- Create demographic table


## Dataset Dictionaries

### I94 Immigration Data

| Name | Description
| :-----: | :-: 
| cicid    | Record ID
| i94yr	   | Year
| i94mon   | Month
| i94ci    | Code - country of birth
| i94res   | Code - country of residence
| i94port  | Port of admission
| arrdate  | Arrival Date in the US
| i94mode  | Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = N/A)
| i94addr  | State of arrival - US
| depdate  | Departure Date - US
| i94bir   | Age of Respondent in Years
| i94visa  | Visa codes collapsed into three categories
| count	   | Field used for summary statistics
| dtadfile | Character Date Field - Date added to I-94 Files
| visapost | Department of State where where Visa was issued
| occup	   | Occupation that will be performed in U.S
| entdepa  | Arrival Flag - admitted or paroled into the U.S.
| entdepd  | Departure Flag - Departed, lost I-94 or is deceased
| entdepu  | Update Flag - Either apprehended, overstayed, adjusted to perm residence
| matflag  | Match flag - Match of arrival and departure records
| biryear  | 4 digit year of birth
| dtaddto  | Character Date Field - Date to which admitted to U.S. (allowed to stay until)
| gender   | Non-immigrant sex
| insnum   | INS number
| airline  | Airline used to arrive in U.S.
| admnum   | Admission Number
| fltno	   | Flight number of Airline used to arrive in U.S.
| visatype | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

### World Temperature Data

| Name | Description
| :-----: | :-: 
| dt                             | Date
| AverageTemperature             | Global average land temperature in celsius
| AverageTemperature Uncertainty | 95% confidence interval around the average
| City	                         | Name of City
| Country	                 | Name of Country
| Latitude	                 | City Latitude
| Longitude                      | City Longitude


### U.S. City Demographic Data

| Name | Description
| :-----: | :-: 
| City	                 | City Name
| State	US               | State where city is located
| Median Age	         | Median age of the population
| Male Population	 | Count of male population
| Female Population	 | Count of female population
| Total Population	 | Count of total population
| Number of Veterans	 | Count of total Veterans
| Foreign born	         | Count of residents of the city that were not born in the city
| Average Household Size | Average city household size
| State Code	         | Code of the US state
| Race	                 | Respondent race
| Count	                 | Count of city's individual per race


## IMPORTANT NOTES:

> Add IAM role info to `config.cfg`

> Code references taken from the GitHub, Stackoverflow and Google searches
