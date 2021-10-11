# truechallenge

## Requirements
- Java SDK 1.8+
- sbt 1.4.3
- Docker
- docker-compose

## Tests
To run the tests you should launch
```
sbt clean test
```
this will compile and launch the unit tests

## The application
### Prepare the environment
First of all check that the port 5432 is free on your computer, otherwise change it in the `docker-compose.yml` file (root of the project) and in the `application.conf`
that you can find in truechallenge/src/main/resources

You should then download the datasets in the root of the project:
* https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv
* https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz

**It's advisable to extract gzipped archive, because in this way Spark will be able to split it and parallelize read and following operations.**

### Run the application
First place yourself in the root of the project and run
```
docker-compose up
```
This will run a postgresql container on localhost:5432. Once the DB is up and running you can run the application with:
```
sbt "runMain com.github.domesc.truechallenge.Main"
```
This will run the Spark application that will populate the DB table named *movies*. Once the job has finished you can query the DB table
either by connecting to the container directly or by using another small tool I wrote which query data with Spark after reading the table from the DB. To do the last you should run:
```
sbt "runMain com.github.domesc.truechallenge.DBQueryApp"
```
and write your query on the command line.

## Tools choices
I chose to use Spark as framework since it is easy to scale the solution and exploit the parallelism of the machine to split/read and process
the files.
Docker to run postgresql avoids any particular installation or configuration of the DB.

## Algorithmic choices
No particular algorithmic choices. The choice in reading the files was between reading them by chunks or in parallel. I exploited Spark parallelization to split
files and transform them in parallel (the more are the cores in your machine the faster will be). By doing this you still need enough memory in your PC,
but it's going to be easier to scale the solution in a cluster of machines.

## Test for correctness
I wrote some unit tests covering at least the logical transformations in the data. We could write also some integration tests to test the end to end solution,
but that takes more time than 2 hours.



