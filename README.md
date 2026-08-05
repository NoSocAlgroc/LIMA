This is the repository for the 2026 VLDB paper: "Discovering Approximate Denial Constraints in Large Databases".


## Algorithm compilation

The compilation of LIMA follows the same structure as most other DC discovery algorithms:

### Dependencies
* Java JDK 1.8 or later
* Maven 3.1.0 or later
* Git
* Having installed Metanome, following the instructions at https://github.com/HPI-Information-Systems/Metanome/tree/master.

### Compilation

The algorithm may be compiled by running the following command from the root directory:

```mvn clean package```

After the process finishes, the executable is found in the ```target``` directory of the algorithm. This executable includes all dependencies and is ready to use.

#### Usage

The algorithm may be executed from console with the following command:

```java -jar LIMA.jar DATASET APPROX ROWS```

Where each parameter is:

* DATASET: path to the dataset.
* APPROX: approximation factor in [0,1).
* ROWS: Number of rows of the dataset to be used. For dynamic evaluation, append tuple sizes as: 100000;1000;25000, and DCs will be discovered on the original slice of tuples and expanded by adding each new amount of tuples, maintainting its significance over the expanded datasets.

## Reproducibility

All experiments are derived from straightforward executions of all algorithms. For convenience, we provide datasets, compiled JARs, and python code to execute the DC discovery algorithms and obtain and compare their results in our DC [repository](https://github.com/NoSocAlgroc/DCValidity).


Figure 5 is the only experimental result outside this common framework. In this case, any traditional DC discovery algorithm can be slighly modified to print the size of the evidence set. Since our algorithm does not compute the full Evidence Set (as a consequence of the results shown in Figure 5), we provide instructions on how to modify the [ECP DC discovery algorithm](https://github.com/EduardoPena/fdcd/tree/main) to obtain the size of the evidence set.

Simply add the following in line 69 on FDCDMocker.java [https://github.com/EduardoPena/fdcd/tree/main](https://github.com/EduardoPena/fdcd/blob/main/src/main/java/br/edu/utfpr/pena/fdcd/mockers/FDCDMocker.java)

```log.info(evidenceSet.size());```
