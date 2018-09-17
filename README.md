# Daily Review System
This project will build a daily review dashboard of 311 service requests among several cities. With new service records being available via APIs, the data transform process is scheduled daily to produce a daily review dashboard describe the performance of each agency that responsible for resolving requested issues.

## Diagram
![diagram](fig/diagram.png)

## Tools explained
[Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/introduction.html): real-time aggregation of data followed by loading the aggregate data into a data warehouse


## Data profiling
