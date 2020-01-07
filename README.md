# udacity-data_pipelines
Submission for Udacity DEND Data Pipelines project.

- [Link to rubric](https://review.udacity.com/#!/rubrics/2478/view)
- [Link to project instructions](https://classroom.udacity.com/nanodegrees/nd027/parts/45d1c3b1-d87b-4578-a6d0-7e86bb5fea6c/modules/2adf57ae-57cb-42f6-bd65-a2c383797ce3/lessons/4d1d5892-2cab-4456-8b1a-fb2b5fa1488d/concepts/last-viewed?contentVersion=2.0.0&contentLocale=en-us)

## How to run the project locally

```
pip install apache-airflow
```

```
./airflow-start.sh
```

Go to `http://localhost:8080` to see this project's DAGs.

From the webserver, go to "Admin > Connections" menu, and then create the following connections:

1. AWS credentials:
    - Conn Id: `aws_credentials`
    - Login: *Your AWS ACCESS KEY*
    - Password: *Your AWS SECRET ACCESS KEY*
2. Redshift connection
    - Conn Id: `redshift`
    - Conn Type: `Postgres`
    - Host: *Endpoint of your Redshift cluster*
    - Schema: `dev`
    - Login: `awsuser`
    - Password: *Password for user when launching your Redshift cluster.*
    - Port: `5439`