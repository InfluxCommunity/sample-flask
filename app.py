#!/usr/bin/python3

# use pip install flask or pip3 install flask
from flask import Flask, request
import plotly.express as px
from datetime import datetime
import json
import os
import requests
from urllib.parse import urljoin

# Import the classes to be used from the InfluxDB client library.
# Use $pip install influxdb-client or $pip3 install influxdb-client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.flux_table import FluxStructureEncoder
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.rest import ApiException

app = Flask(__name__)

# Your app needs the following information:
    # An organization name
    # A host URL
    # A token
    # A bucket name

# Your organization name. An organiztion is how InfluxDB groups resources such as tasks, buckets, etc...
organization = os.environ["INFLUXDB_ORGANIZATION"]

# The host URL for where your instance of InfluxDB runs. This is also the URL where you reach the UI for your account.
host = os.environ["INFLUXDB_HOST"] 

# An appropriately scoped token or set of tokens. For ease of use in this example, we will use an all access token.
# Note that you should not store the token in source code in a real application, but rather use a proper secrets store.
# More information about permissions and tokens can be found here:
# https://docs.influxdata.com/influxdb/v2.1/security/tokens/
token = os.environ["INFLUXDB_TOKEN"]

# A bucket name is required for the write_api. A bucket is where you store data, and you can 
# group related data into a bucket. You can also scope permissions to the bucket level as well.
bucket_name = os.environ["INFLUXDB_BUCKET"]

# Instantiate the client library
client = InfluxDBClient(url=host, token=token, org=organization)

# Instantiate the write and query apis
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

@app.route("/")
def index():
    return """<p>Welcome to your first InfluxDB Application</p>
    """

@app.route("/ingest", methods=["POST"])
def ingest():
    # Post the following data to this function to test
    # {"user_id":"user1", "measurement":"measurement1","field1":1.0}

    # A point requires at a minimum: A measurement, a field, and value.
    # A measurement is the top level organization for points in a bucket. Similar to a table in a sql database.
    # a field and its related value or similar to a column and value in a sql database.
    # user_id will be used to "tag" each point, so that your queries can easily find the data for each seperate user.

    # You can write any number of tags and fields in a single point, but only one measurement
    # To understand how measurements, tag values, and fields define points and series, follow this link:
    # https://awesome.influxdata.com/docs/part-2/influxdb-data-model/

    # For learning about how to ingest data at scale, and other details, follow this link:
    # https://influxdb-client.readthedocs.io/en/stable/usage.html#write

    # Your real code should authorize the user, and ensure that the user_id matches the authorization.
    user_id = request.json["user_id"]
    measurement = request.json["measurement"]
    value = request.json["field1"]

    point = Point(measurement) \
        .field("field1", value) \
        .tag("user_id", user_id) \
        .time(datetime.utcnow(), WritePrecision.NS)

    # If you don't want to post the data, you can uncomment the following, and delete the above code    
    # point = Point("measurement1") \
    #     .field("field1", 1.0) \
    #     .tag("user_id", "user1")\
    #     .time(datetime.utcnow(), WritePrecision.NS)

    try:
        write_api.write(bucket_name, organization, point)
        return {"result":"data accepted for processing"}, 200
    except InfluxDBError as e:
        if e.response.status == "401":
            return {"error":"Insufficent permissions"}, e.response.status
        if e.response.status == "404":
            return {"error":f"Bucket {bucket_name} does not exist"}, e.response.status

    # To view the data that you are writing in the UI, you can use the data explorer
    # Follow this link: {need to wait for /me/ to ship for this to work}

@app.route("/query", methods=["POST"])
def query():
    # Return all of the data for the user in the last hour in json format
    # Post the following to this enpoint:
    # {"user_id":"user1"}

    # Your real code should authorize the user, and ensure that the user_id matches the authorization.
    user_id = request.json["user_id"]
    # uncomment the following line and comment out the above line if you prefer to try this without posting the data
    # user_id = "user1"

    # Queries are written in the javsacript-like Flux language
    # Simple queries are in the format of from() |> range() |> filter()
    # Flux can also be used to do complex data transformations as well as integrations.
    # Follow this link to learn more about using Flux:
    # https://awesome.influxdata.com/docs/part-2/introduction-to-flux/

    # Set up the arguments for the query parameters
    params = {"bucket_name":bucket_name, "user_id":user_id}
    query = "from(bucket: bucket_name) |> range(start: -1h) |> filter(fn: (r) => r.user_id == user_id)"

    # Execute the query with the query api, and a stream of tables will be returned
    # If it encounters problems, the query() method will throw an ApiException.
    # In this case, we are simply going to return all errors to the user but not handling exceptions
    tables = query_api.query(query, org=organization, params=params)

    # the data will be returned as Python objects so you can iterate the data and do what you want
    for table in tables: 
        for record in table.records:
            print(record)

    # You can use the built in encoder to return results in json
    output = json.dumps(tables, cls=FluxStructureEncoder, indent=2)
    return output, 200

@app.route("/visualize", methods=["GET"])
def visualize():
    # This function returns a visualization (graph) instead of just data
    
    # Your real code should authorize the user, and ensure that the user_id matches the authorization.
    # Send the user name as an argument from the web browser:
    # 127.0.0.1:5001/visualize?user_name=user1
    user_id = request.args.get("user_name")

    # uncomment the following line and comment out the above line if you prefer to try this without posting the data
    user_id = "user1"

    # Query using Flux as in the /query end point
    params = {"bucket_name":bucket_name, "user_id":user_id}
    query = "from(bucket: bucket_name) |> range(start: -1h) |> filter(fn: (r) => r.user_id == user_id)"

    # This example users plotly and pandas to create the visualization
    # You can learn more about using InfluxDB with Pandas by following this link:
    # 
    # InfluxDB supports any visualization library you choose, you can learn more about visualizing data following this link:
    # 
    data_frame = query_api.query_data_frame(query, organization, params=params)
    graph = px.line(data_frame, x="_time", y="_value", title="my graph")
    
    return graph.to_html(), 200

@app.route("/alerts", methods=["POST, GET, DELETE, PUT"])
def alerts():
    # This function uses InfluxDB's task system to provide a very simple alerting feature.
    # InfluxDB has a built in Checks and Notifications sytstem that is highly configurable.
    # For information about creating Checks and Notifications in the InfluxDB UI and other information,
    # follow this link:
    # For details about 

    # Your code should never poll InfluxDB for status, and your code should never periodically run
    # queries. You should let InfluxDB's task system schedule and run those for you, and you can
    # add callbacks and notification endpoints to Flux.
    if request.method == "POST":
        # create a new task
        pass
    if request.method == "GET":
        # return list of alerts for user
        pass
    if request.method == "DELETE":
        # delete the task
        pass
    if request.method == "PUT":
        #update the task
        pass

@app.route("/monitor")
def monitor():
    # This page returns information about influxdb operations

    # InfluxDB includes functionality designed to help you programatically manage your instances.
    # This page provides basic insights into usage, and tasks. Much more related functionality exists.
    # There is a template that you can install into your account, to learn more, follow this link:
    # https://www.influxdata.com/blog/tldr-influxdb-tech-tips-using-and-understanding-the-influxdb-cloud-usage-template/

    # Your own code should verify that the person viewing this has proper authorization

    # The following flux query will retrieve the 3 kinds of usage data available 
    # and combine the data into a single table for ease of formatting. 
    # For more information about the usage.from() function, see the following:
    # https://docs.influxdata.com/flux/v0.x/stdlib/experimental/usage/from/

    query = """
import "experimental/usage"

usage.from(start: -1h, stop: now())
|> toFloat()
|> group(columns: ["_measurement"])
|> sum()
    """
    tables = query_api.query(query, org=organization)
    html = "<H1>usage</H1><TABLE><TR><TH>usage type</TH><TH>value</TH></TR>"
    for table in tables:
        for record in table:
            mes = record["_measurement"]
            val = record["_value"]
            html += f"<TR><TD>{mes}</TD><TD>{val}</TD></TR>"
    html += "</TABLE>"

    # This part of the function looks at task health. 
    # Tasks allow you to run code periodically within InfluxDB. For an overview of tasks, see the following:
    # https://docs.influxdata.com/influxdb/cloud/process-data/get-started/
    
    # It is very useful to know if your tasks are running and succeeding or not, and to alert on those conditions.
    # This section uses the tasks_api that comes with the client library
    # Documentation on this library is available here:
    # https://influxdb-client.readthedocs.io/en/stable/api.html#tasksapi
    tasks_api = client.tasks_api()

    # list all the tasks
    tasks = tasks_api.find_tasks()

    html += "<H1>tasks</H1><TABLE><TR><TH>name</TH><TH>status</TH><TH>last run</TH><TH>last run status</TH></TR>"
    
    # for active tasks, format the status report
    for task in tasks:
        started_at = ""
        run_status = ""
        if task.status == "active":
            run = tasks_api.get_runs(task.id, limit=1)[0]
            started_at = run.started_at
            run_status = run.status
        html += f"<TR><TD>{task.name}</TD><TD>{task.status}</TD><TD>{started_at}</TD><TD>{run_status}</TD></TR></BR>"

    if len(tasks) == 0:
        html += "<TR><TD>no tasks</TD><TR>"
    html += "</TABLE>"

    return html, 200

def register_invokable_script():
    # This function will store your query in influxdb, and return an id
    # You can then invoke the script and pass arguments for the parameters
    pass

def bucket_check():
    # this function checks if the desired bucket exits, and creates it if needed

    # A bucket is where you store data for your organization. A bucket has a retention
    # policy, which determines how long the bucket will retain data. Data older than the retention
    # policu will automatically be deleted and cleaned up by InfluxDB.
    # You can read more about buckets and retention policy by following this link:
    # https://docs.influxdata.com/influxdb/v2.1/organizations/buckets/
    
    try:
        # use the buckets api to find a bucket by its name
        b = client.buckets_api().find_bucket_by_name(bucket_name)
        print(f"bucket ({bucket_name}) found with retention policy: {b.rp}")
    except ApiException as e:
        # The most likely problem is that the bucket does not exist, in which case add it
        # Check the status code for other possible errors that you wish to handle
        if e.status == 404:
            print(f"bucket {bucket_name} not found, creating it")
            client.buckets_api().create_bucket(bucket_name=bucket_name)
        if e.status == 401:
            print(f"Insufficent permsissions, exiting")
            exit()

if __name__ == '__main__':
    bucket_check()
    app.run(host='0.0.0.0', port=5001, debug=True) # using port 5001, because MacOS has started listening to 5000
