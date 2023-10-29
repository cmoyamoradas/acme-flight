# Acme Flights

Acme Flights is an application that processes some flight data to provide the following results;
- Total number of flights per year month.
- List of 100 most frequent flyers.
- List of passenger ids with the greatest number of countries visited in without being in the UK.
- List of passengers who have been on more than 3 flights together.

## Implementation

Acme Flights is implemented in Java (JDK 1.8), using Maven as package manager. Implementation relies on the following libraries:
- Spark 2_12 (v2.4.8)
- Picocli (v4.7.5)
- Spring Boot (v2.7.11)

## Build the application

To build the application, run the following command from the root folder of the project:
```bash
 mvn clean package
```
---
**Note for the build**

Look at the `<plugins>` section in the `pom.xml` file to see how the Spring Boot Maven plugin has been configured, first, to specify the main class and, second, to add `spring-boot:repackage` as part of the `package` phase of the Maven lifecyle.

In case you're working with JDK 11+, the manifest file needs to contain, apart from the reference to the main class, some `add-opens` arguments that we need to pass to the JVM. If you want to run the application with the Spring Boot Maven plugin (spring-boot:run), you'll have to add the same --add-opens as JVM arguments

---

### Run the application

After building the application, you will find an executable jar file (`acme-flight-1.0.jar`) under the `target/` subfolder.

## Usage
Normal usage leverage the Picocli library and follows this pattern:
```bash
 java -jar acme-flight-1.0.jar [command] [option]
```
To know the list of available commands, run the parent command with the help option:
```bash
java -jar target/acme-flight-1.0.jar -h

Usage: <main class> [-h] [COMMAND]
Acme Flights is an application that processes flight data
  -h, --help   Usage
Commands:
  longest-run        List of passenger ids with the greatest number of
                       countries visited in without being in the UK
  flights-per-month  Total number of flights per year month
  flying-together    List of passengers who have been on more than 3 flights
                       together
  frequent-flyers    List of the 100 most frequent flyers
```
Choose any command and run it with the help option. Example:
```bash
java -jar target/acme-flight-1.0.jar flights-per-month -h

Usage: <main class> flights-per-month [-h] -f=<inputPath> [-o=<outputPath>]
Total number of flights per year month
  -f, --flights-file=<inputPath>
               Provide the flights data [REQUIRED]
  -h, --help   Usage
  -o, --output-file=<outputPath>
               Write the output in a file [OPTIONAL]
```
## Warnings with JDK 11+
When working with Spark and JDK 11+ you may get the following WARN when running the application:
```bash
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (jar:file:/Users/carlosmm/Workspace/acme-flight/target/acme-flight-1.0.jar!/BOOT-INF/lib/spark-unsafe_2.12-2.4.8.jar!/) to method java.nio.Bits.unaligned()
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
```
Refer to the following links for further details:
- https://issues.apache.org/jira/browse/SPARK-34095
- https://issues.apache.org/jira/browse/SYSTEMDS-3229