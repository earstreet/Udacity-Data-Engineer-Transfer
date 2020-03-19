# Project 6: Data Engineering Capstone Project

### Overview

The purpose of the data engineering capstone project is to give me a chance to combine what I've learned throughout the program.

##### Udacity Provided Project

In the Udacity provided project, I'll work with four datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. 

### Instructions

The project is broken down into a series of steps.

#### Step 1: Scope the Project and Gather Data

Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, I’ll:

- Identify and gather the data I'll be using for my project (at least two sources and more than 1 million rows). 
- Explain what end use cases I'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

#### Step 2: Explore and Assess the Data

- Explore the data to identify data quality issues, like missing values, duplicate data, etc.
- Document steps necessary to clean the data

#### Step 3: Define the Data Model

- Map out the conceptual data model and explain why I chose that model
- List the steps necessary to pipeline the data into the chosen data model

#### Step 4: Run ETL to Model the Data

- Create the data pipelines and the data model
- Include a data dictionary 
- Run data quality checks to ensure the pipeline ran as expected

#### Step 5: Complete Project Write Up

- What's the goal? What queries will I want to run? How would Spark or Airflow be incorporated? Why did I choose the model you chose?
- Clearly state the rationale for the choice of tools and technologies for the project.
- Document the steps of the process.
- Propose how often the data should be updated and why.
- Post my write-up and final data model in a GitHub repo.
- Include a description of how you would approach the problem differently under the following scenarios:
  - If the data was increased by 100x.
  - If the pipelines were run on a daily basis by 7am.
  - If the database needed to be accessed by 100+ people.

## Getting Started

The project contains the following files:

[data](./data): Folder with example data to run the scripts on the local computer.

[images](./images): Folder with images from the data schema.

[etl.py](./etl.py): Script to run the ETL process.

[dl.cfg](./dl.cfg): Config file with parameters to run the scripts.

[Capstone_Project.ipynb](./Capstone_Project.ipynb): Jupyter Notebook with all the steps in [etl.py](./etl.py). 

### Prerequisites

- The script uses PySpark, a high-level API for Spark in Python which can be installed with the following commands.

  ```python
  pip install pyspark
  ```

  You also need access to the AWS services:

  - EC2
  - IAM
  - S3
  - EMR

### Starting

In order to run the scripts you have to do the following steps in the terminal console.

Start the ETL process to load the data, create the tables and save them on another S3-bucket:

```bash
python etl.py
```
