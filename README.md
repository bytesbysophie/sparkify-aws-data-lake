# Creating a Data Lake on AWS using Spark for the Music Streaming Service Sparkify

## Table of Contents
1. [Project Summary](#Project-Sumary)
2. [Installation](#Installation)
3. [File Descriptions](#File-Descriptions)
4. [Authors and Acknowledgements](#Authors-Acknowledgements)

## Project Summary <a name="Project-Sumary"></a>
The analytics team of the fictional music streaming service Sparkify faces more and more data as the service grows.
Thus, they now need to move their data warehouse to a data lake solution.
The technical requirements for this project state, that an etl pipeline for a data lake hosted on S3 should be build:
1. Data should be extracted from S3
2. Data should be transformed and inserted into analytics tables using Sparl
3. The analyitcs tables should be loaded back into S3

## Installation <a name="Installation"></a>
* Add your AWS keys to dl_template.cfg file and save it as dl.cfg (don't use quotes and make sure that you never share the inserted keys publicly).
* Make sure all libraries imported in etl.py are installed in your Python environment
* Change "output_data" (line 137 in etl.py) to the bucket/ path of your choice
* Run the 'python etl.py' in order to trigger the etl process

## File Descriptions <a name="File-Descriptions"></a>
* dl_template.cfg: Template for the aws configration (credentials should be added + files saved as dl.cfg)
* .gitignore: Includes dl.cfg to prevent users from pushing the configuration files including their credentials
* etl.py: Implements the ETL process

## Authors and Acknowledgements <a name="Authors-Acknowledgements"></a>
This project has been implemented as part of the Udacity Data Engineering Nanodegree program. The data has been provided by Udacity accordingly as well as the project structure/ file templates.
