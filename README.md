# 411for311
| ->  [Demo](http://www.411for311.fun/)        |                ->  [Slides](https://docs.google.com/presentation/d/1kONgmG1dGofYOhYfl05p5McKBf3he54KOfxXNhjmhMk/edit?usp=sharing)           |
| ------------- |:-------------:|

![demo](fig/demo.gif)
__Figure 1.__ Final Dashboard on Tableau. In this dashboard, all blue components means recent records, green means historical, purple means the difference between recent and historical. When a particular department being selected, only the data of this department will be rendered on the heat maps, historical line plot, and recent average count.

## 1. Overview
This is a daily review dashboard showing 311 complaints resolution performance for government agencies. With new complaint records being available via API, the data transform process is scheduled daily to extract the new data and produce a daily review dashboard describing the performance of each agency who is responsible for resolving the requested issues, compared with the historical records.

![diagram](fig/diagram.png)
__Figure 2.__ Data Flow Diagram. For historical data processing, the EMR was utilized once. For the daily updating process, CloudWatch is the trigger for the whole updating process, and data flows are driven by Lambda functions.

#### Ideas behind the pipeline design
- Cost efficient. As data are processed once daily, using Lambda functions (pay only for what you use) helps reduce the cost.
- Handling the API unavailability. As the data source API is under  maintenance monthly, the pipeline should __1)__ not crash when the API is unavailable and __2)__ be able to process all unprocessed data when the API is back.
- Extensible and scalable. With Kinesis as the ingestion layer, this pipeline can accept more data producers (e.g. adding other APIs) and more data consumers. Meanwhile, Lambda functions, Kinesis, S3, and Redshift are all easily scalable ready for future growth of data.

## 2. Requirements
- Python3
- [AWS CLI](https://aws.amazon.com/cli/)

## 3. Installation
- [EMR](src/emr/emr.sh)
- [Kinesis](src/kinesis/create-stream.py)
- [Lambda](src/lambda/create_function.sh)
- [RedShift](src/redshift/create_cluster.sh)
- [Tableau](https://www.tableau.com/)

## 4. Data Source
[311 complaints dataset](https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) is available in NYC Open Data. Data can be accessed via [Socrate API](https://dev.socrata.com/foundry/data.cityofnewyork.us/fhrw-4uyv).

## 5. Getting Started
```
bash ./run_cloudwatch_trigger.sh
```
