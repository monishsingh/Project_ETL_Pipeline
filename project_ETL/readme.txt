STEDI Human Balance Analysis project

This zip file contains a project folder inside three separate folders - landing, trusted, and curated. All these folders contain separated folders of images like screenshots and code folders contain Python scripts that are downloaded from aws.

1. Firstly I copy the JSON data of the customer, accelerometer, and step trainer in a particular s3 location.
2. Create a separate table for customer_landing, accelerometer_landing, and step_trainer_landing by using 'create a table from s3' manually.
3. Mostly filter the data of customer, accelerometer, and step_trainer by using glue ETL job and I Attached the transformation Python script for that.
4. Create a table like customer_trusted, accelerometer_trusted, step_trainer_trusted, machine_learning_curated by using crawlers inside the athena services.
  