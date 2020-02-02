# Comments on solution
The solution unfortunately is not complete.
I did not manage to use properly scalaz and cats as I am not yet very familiar with those
frameworks.

My idea to solve the problem was to read the files one by one. For every file, I instantiate class
SensorsStatistcs which has sensorsInfo (per sensor id) + globalInfo (containing general info about
totalfiles processes, total count, total failed). Global info are of object class.
SensorsStatics are updated for every line of leader report data found.
The main problem here is calculation of average, which cannot be done on-fly.
So partially I wanted to store total sum of all valid values, then with the count of all
measurements during the process of mergeWith (using cats and or scalaz) I could calculate during
merge process final statistics.

Unfortunately I did not manage to make it work.
Tests have not yet been added, as the task is not complete.
My testing was basing on two sample data sets.

Project is written through maven - see usage below

# Usage
- mvn test
- mvn exec:java -Dexec.args="path_to_csv_files"

# Sensor Statistics Task

Create a command line program that calculates statistics from humidity sensor data.

### Background story

The sensors are in a network, and they are divided into groups. Each sensor submits its data to its group leader.
Each leader produces a daily report file for a group. The network periodically re-balances itself, so the sensors could 
change the group assignment over time, and their measurements can be reported by different leaders. The program should 
help spot sensors with highest average humidity.

## Input

- Program takes one argument: a path to directory
- Directory contains many CSV files (*.csv), each with a daily report from one group leader
- Format of the file: 1 header line + many lines with measurements
- Measurement line has sensor id and the humidity value
- Humidity value is integer in range `[0, 100]` or `NaN` (failed measurement)
- The measurements for the same sensor id can be in the different files

### Example

leader-1.csv
```
sensor-id,humidity
s1,10
s2,88
s1,NaN
```

leader-2.csv
```
sensor-id,humidity
s2,80
s3,NaN
s2,78
s1,98
```

## Expected Output

- Program prints statistics to StdOut
- It reports how many files it processed
- It reports how many measurements it processed
- It reports how many measurements failed
- For each sensor it calculates min/avg/max humidity
- `NaN` values are ignored from min/avg/max
- Sensors with only `NaN` measurements have min/avg/max as `NaN/NaN/NaN`
- Program sorts sensors by highest avg humidity (`NaN` values go last)

### Example

```
Num of processed files: 2
Num of processed measurements: 7
Num of failed measurements: 2

Sensors with highest avg humidity:

sensor-id,min,avg,max
s2,78,82,88
s1,10,54,98
s3,NaN,NaN,NaN
```

## Notes

- Single daily report file can be very large, and can exceed program memory
- You can use any Open Source library
- Program should only use memory for its internal state (no disk, no database)
- Sensible tests are welcome

