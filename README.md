# Data Science Correlation Experiment - Correlation between LA Crimes and Museum Visitors
This repository contains an experiment in which I tested the correlation between the Crimes number in LA and the number of Museum Visitors in the same city.

## Experiment Datasource
n this experiment, I used two datasets from open-access repositories in order to get the information needed for my experiment. Those data sets were:
1. LA_Crime_Data_from_2010_to_Present.csv
2. LA_museum_visitors.csv

Both input and generated data were preserved on Zenodo using this DOI citation: [10.5282/dmp.LAexperiment.data](https://zenodo.org/record/2647620/files/LAExperiment_data.zip)

## Prerequisites
This experiment was performed using JavaSE 11.0 with Apache Spark 2.4 "It's a Maven project, so don't worry about versions". Other libraries were used and will be listed in the Prerequisites section.

## Setup and Running
In order to setup and run this experiment, you can follow the next steps:

1. Clone this repository to your machine
```
git clone https://github.com/HasanKhatib/DS-CrimesAndMuseumsInLA.git
```
2. Clone dataset repository from Bitbucket to the same project folder, it will add a folder named `laexperiment_data`
```
git clone https://hasanalkhatib@bitbucket.org/hasanalkhatib/laexperiment_data.git
```
Or, just download data using the DOI citation from above to `laexperiment_data` folder.

3. Generate sources from the maven `pom.xml` file

4. Run it!

### Run using Docker
You can also find a docker file in `target/docker` document in the project. Use it to build Docker image.

## Experiment Process
In the next UML, you will notice the flow of data in the experiment and how the output CSV file was generated. In general, the steps of the process were as follows:
1. Read CSV files as arrays of string.
2. Splitting each line to several data attributes, in some cases I needed to work with regex to split lines that have commas within data attributes.
3. Filtering data from both datasets to match the period of studying.
4. Group results by year, and aggregate data by summing/counting it.
5. Populate new CSV file by writing the data into it.

![Experiment UML](https://user-images.githubusercontent.com/1809095/56473698-06d0b780-646f-11e9-9cf5-0e1db18d56ef.png)

## Results
As a result of this experiment, I generated a CSV file named `LA_MuseumVisitorsAndCrimes.csv` located in `laexperiment_data\output`. Adding to that, the implementation will generate a chart bar when you run it.

![LAExperiment_chart](https://user-images.githubusercontent.com/1809095/56473803-ba867700-6470-11e9-9812-680a0c126296.png)


## License
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
