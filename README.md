
RecordsFileProducer generates transaction records (purchases) into a file in json format with the following structure:
 * id: transaction ID (string)
 * transaction_timestamp: transaction's creation time in format `yyyy-MM-dd HH:mm:ss` (string)
 * account: pseudo-random account number (string)
 * amount: random amount of a transaction (string)
 * category: purchase's category
 
 Example: `{"id":"40","transaction_timestamp":"2020-07-21 15:17:50","account":"123-ABC-788","amount":"465.77","category":"Movies"}`
 

FileProducer accepts 2 arguments:
  * linesCount - number of transactions to generate
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

    Example: `java -jar dq-file-producer.jar 100 /Users/foo/bar/transactions.txt`

This would generate 100 transaction records into a file `/Users/foo/bar/transactions.txt`