
RecordsFileProducer generates random hourly weather data into a file in json format with the following structure:
 * station: station ID (string). Currently possible values are `55511100222` and `33399977444`
 * date: timestamp of the collected weather data in a format `yyyy-MM-dd HH:mm:ss` (string). The beginning date is `2020-01-01 00:00:00`
 * hourly_temperature: pseudo-random hourly Fahrenheit temperature (integer)
 * hourly_humidity: pseudo-random hourly humidity between 0 and 100 (integer)
 * hourly_visibility: pseudo-random hourly visibility between 0 and 10 (integer)
 * hourly_wind_speed: pseudo-random hourly wind speed (integer)
 
 Example: `{"station":"55511100222","date":"2020-01-08 05:00:00","hourly_temperature":"37","hourly_humidity":"92","hourly_visibility":"10","hourly_wind_speed":"2"}`
 
 ### Note
 RecordsFileProducer pseudo-randomly generates abnormal values of: 
 - `hourly_temperature` (possible values: 90, 100, 110) 
 - `hourly_humidity` (possible values: 0, 5, 10) 
 - `hourly_visibility` (possible values: 0, 1) 
 - `hourly_wind_speed` (possible valuess: 40, 50, 60) 
 
 which might occur at least once in 24h (usually more often)
 

RecordsFileProducer accepts 2 arguments:
  * linesCount - number of records to generate
  * file - absolute name of the file to write to

## Setup

### Preconditions

-   Java >= 1.8
-   Scala >= 2.11
-   sbt >= 1.3.8

### Steps

#### 1. Get project code

`git clone git@github.com:freedomserg/dq-records-file-producer.git`

#### 2. Execute a producer

In terminal:

-   Go to the project dir: `cd dq-records-file-producer`
-   Build a jar file: `sbt clean assembly`
-   Run generated jar file:  `java -jar dq-file-producer.jar {linesCount} {file}`

    Example: `java -jar dq-file-producer.jar 100 /Users/foo/bar/weather-data.txt`

This would generate 100 transaction records into a file `/Users/foo/bar/weather-data.txt`