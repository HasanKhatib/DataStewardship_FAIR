# Data Science Correlation Experiment - Correlation between LA Crimes and Museum Visitors
This repository contains an experiment in which I tested the correlation between the Crimes number in LA and the number of Museum Visitors in the same city.

## Experiment Datasource
In this experiment I used two datasets from open-access repositories in order to get the information needed for my experiment. Those data sets were:
1. LA_Crime_Data_from_2010_to_Present.csv
2. LA_museum_visitors.csv

Both input and generated data were preserved on Zenodo using this DOI citation: [10.5282/dmp.LAexperiment.data](https://doi.org/10.5282/dmp.LAexperiment.data)

## Prerequisites
This experiment was performed using JavaSE 11.0 with Apache Spark 2.4 "It's a Maven project, so don't worry about versions". Other libraries were used and will be listed in Prerequisites section.

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

## Experiment Process

![Experiment UML](https://user-images.githubusercontent.com/1809095/56473698-06d0b780-646f-11e9-9cf5-0e1db18d56ef.png)


## License
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
