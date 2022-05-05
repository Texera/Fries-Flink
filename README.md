# Fries on Apache Flink
This is the code for the implementation of Fries on Apache Flink 1.13. More details about Flink and how to build it can be found at [Flink's github](https://github.com/apache/flink).

## Building the project
```console
git clone https://github.com/shengquan-ni/Fries-Flink.git
cd Fries-Flink
mvn clean package -Drat.numUnapprovedLicenses=1000 -DskipTests -Dfast -Dcheckstyle.skip
```
## Running the project:

This step is the same as Flink's original instruction. Fries is already a built-in functionality of Flink in this repo. To enable Fries, you can pass the following parameters as JVM params(start with"-D") in flink-conf.yaml

## Resahpe parameters:

Some parameters of Fries can be tuned:
| Parameter name  | Type | Default value | Usage |
| ------------- | ------------- |  ------------- |  ------------- |  
| repeatedReconfiguration  | Boolean      |  false  | repeat same reconfiguration after a given time interval | 
| reconfInitialDelay  | Long  |  5000  | Delay of the first reconfiguration (ms) | 
| reconfInterval  | Long  |  10000  | Interval between 2 reconfigurations (ms). Only have effect when repeatedReconfiguration = true| 
| reconfTargets  | String      |  ""  | Reconfiguration operators in the job, saparated with comma. | 
| oneToMany  | String      |  ""  | One-to-Many operators in the job, saparated with comma. | 
| reconfScheduler  | String      |  "epoch"  | The reconfiguration scheduler. options: epoch/fries | 
