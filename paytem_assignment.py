### Step 1 - Setting Up the Data
# [x] 1. Load the global weather data into your big data technology of choice.
weather = spark.read.load(
    "/Users/xzhang/Downloads/paytmteam-de-weather-challenge-b01d5ebbf02d/data/2019/part-00000-890686c0-c142-4c69-a744-dfdc9eca7df4-c000.csv.gz",
    format="com.databricks.spark.csv",
    header="true",
    inferSchema="true",
)


# [x] 2. Join the stationlist.csv with the countrylist.csv to get the full country name for each station number.
country = spark.read.load(
    "/Users/xzhang/Downloads/paytmteam-de-weather-challenge-b01d5ebbf02d/countrylist.csv",
    format="com.databricks.spark.csv",
    header="true",
    inferSchema="true",
)
station = spark.read.load(
    "/Users/xzhang/Downloads/paytmteam-de-weather-challenge-b01d5ebbf02d/stationlist.csv",
    format="com.databricks.spark.csv",
    header="true",
    inferSchema="true",
)
fullname = country.join(station, on="COUNTRY_ABBR")

# [x] 3. Join the global weather data with the full country names by station number.
weather = weather.withColumnRenamed("STN---", "STN_NO")
fullweather = fullname.join(weather, on="STN_NO")

### Step 2 - Questions
# [x] 1. Which country had the hottest average mean temperature over the year?
avg_weather = fullweather.groupBy("COUNTRY_FULL").avg()
hotest_country = avg_weather.sort("avg(TEMP)", ascending=False).limit(1)

# [x] 2. Which country had the coldest average mean temperature over the year?
coldest_country = avg_weather.sort("avg(TEMP)", ascending=True).limit(1)

# [x] 3. Which country had the second highest average mean wind speed over the year?
windy_country = avg_weather.sort("avg(WDSP)", ascending=False).limit(2)
second_windy_country = windy_country.sort("avg(WDSP)", ascending=True).limit(1)

# [x] 4. Which country had the most consecutive days of tornadoes/funnel cloud formations?
tfc_days = fullweather.filter(fullweather['FRSHTT']%2 ==1)
most_tornado_country = tfc_days.select(tfc_days['COUNTRY_FULL']).groupBy('COUNTRY_FULL').count().sort('count', ascending=False).limit(1)




# always check with dataset.printSchema()
