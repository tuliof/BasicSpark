
# Basic Spark IntelliJ

This project has only 2 dependencies, Spark Core and Spark SQL.

It also includes a working example using RDDs.

## Running the example

### Running by IntelliJ Idea

Edit the run configuration and fill the _Program arguments_ field with the arguments.
The WordsAnalytics object expects 3 arguments to function, arguments expected are:

1. input-text-file-path
2. output-folder-path-1
3. output-folder-path-2

For argument #1 you can use the file _resources/poem_1.txt_

### Running by sbt shell

1. Open the sbt shell
2. Replace the command below with the appropriate path and run it

> $ runMain exercises.WordsAnalytics \<path to txt file\> \<path to output folder #1\> \<path to output folder #2\>

Example:
> $ runMain exercises.WordsAnalytics \<Path to project folder\>/src/main/resources/poem_1.txt C:/temp/results/1 C:/temp/results/2

## Error

> Error: Could not find or load main class examples.WordsAnalytics

In case you see the error above, open sbt shell (Ctrl+Shift+S) and run the task _compile_.

# To Do

Include a working example using Spark SQL and DataFrames.
