#!/usr/bin/python3

# use pip install flask or pip3 install flask
from flask import Flask, request
import plotly.express as px
from datetime import datetime
import json
import os

# Import the classes to be used from the InfluxDB client library.
# Use $pip install influxdb-client or $pip3 install influxdb-client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.flux_table import FluxStructureEncoder
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.rest import ApiException

app = Flask(__name__)

# Your app needs the following information:
    # An organization id (org_id)
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

    query = f"from(bucket: \"{bucket_name}\") |> range(start: -1h) |> filter(fn: (r) => r.user_id == \"{user_id}\")"

    # Execute the query with the query api, and a stream of tables will be returned
    # If it encounters problems, the query() method will throw an ApiException.
    # In this case, we are simply going to return all errors to the user but not handling exceptions
    tables = query_api.query(query, org=organization)

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
    query = f"from(bucket: \"{bucket_name}\") |> range(start: -1h) |> filter(fn: (r) => r.user_id == \"{user_id}\")"

    # This example users plotly and pandas to create the visualization
    # You can learn more about using InfluxDB with Pandas by following this link:
    # 
    # InfluxDB supports any visualization library you choose, you can learn more about visualizing data following this link:
    # 
    data_frame = query_api.query_data_frame(query, organization)
    graph = px.line(data_frame, x="_time", y="_value", title="my graph")
    
    return graph.to_html(), 200

@app.route("/alerts", methods=["POST, GET, DELETE, PUT"])
def alerts():
    # This function uses InfluxDB's task system to provide a very simple alerting feature.
    # InfluxDB has a built in Checks and Notifications sytstem that is highly configurable.
    # For information about creating Checks and Notifications in the InfluxDB UI and other information,
    # follow this link:
    # For details about 
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
    # This function returns information about how healthy your app is in the InfluxDB backend
    # check for failed tasks
    # bytes written and read in the last hour from bucket_name
    pass

def register_invokable_script():
    # This function will store your query in influxdb, and return an id
    # You can then invoke the script and pass parameters
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
