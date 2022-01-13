#!/usr/bin/python3

# use pip install flask or pip3 install flask
from flask import Flask, request
from datetime import datetime
import json
import os

# Import the classes to be used from the InfluxDB client library.
# Use $pip install influxdb-client or $pip3 install influxdb-client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.flux_table import FluxStructureEncoder

app = Flask(__name__)

# Your app needs the following information:
    # An organization id (org_id)
    # A host URL
    # A token
    # A bucket name

# Your organization name. An organiztion is how InfluxDB groups resourcecs such as tasks, buckets, etc...
organization = "rick+plantbuddy@influxdata.com"

# The host URL for where your instance of InfluxDB runs. This is also the URL where you reach the UI for your account.
host = "https://eastus-1.azure.cloud2.influxdata.com/"

# An appropriately scoped token or set of tokens. For ease of use in this example, we will use an all access token.
# Note that you should not store the token in source code in a real application, but rather use a proper secrets store.
# More information about permissions and tokens can be found here:
# https://docs.influxdata.com/influxdb/v2.1/security/tokens/
token = os.environ["INFLUXDB_TOKEN"]

# A bucket name is required for the write_api. A bucket is where you store data, and you can 
# group related data into a bucket. You can also scope permissions to the bucket level as well.
bucket="default"

# Instantiate the client library
client = InfluxDBClient(url="https://eastus-1.azure.cloud2.influxdata.com", token=token, org=organization)

# Instantiate the write and query apis
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

@app.route("/")
def index():
    return """<p>Welcome to your first InfluxDB Application</p>
    <p>This is a simple sample application to demonstrate the basics of writing a time series application using InfluxDB as a backend.
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

    # If you don't want to post the data, you can uncomment the following    
    # point = Point("measurement1") \
    #     .field("field1", 1.0) \
    #     .tag("user_id", "user1")\
    #     .time(datetime.utcnow(), WritePrecision.NS)

    try:
        write_api.write(bucket, organization, point)
        return {"result":"data accepted for processing"}, 200
    except Exception as e:
        return {"result":e}, 500
    
    # To view the data that you are writing in the UI, you can use the data explorer
    # Follow this link: {need to wait for /me/ to ship for this to work}

@app.route("/query", methods=["POST"])
def query():
    # Return all of the data for the user in the last hour in json format
    # Post the following to this enpoint:
    # {"user_id":"user1"}

    # Queries are written in the javsacript-like Flux language
    # Simple queries are in the format of from() |> range() |> filter()
    # Flux can also be used to do complex data transformations as well as integrations.
    # Follow this link to learn more about using Flux:
    # https://awesome.influxdata.com/docs/part-2/introduction-to-flux/

    # Your real code should authorize the user, and ensure that the user_id matches the authorization.
    user_id = request.json["user_id"]

    query = f"from(bucket: \"default\") |> range(start: -1h) |> filter(fn: (r) => r.user_id == \"{user_id}\")"

    # uncomment the following if you prefer not to try this without posting the data
    # query = f"from(bucket: \"default\") |> range(start: -1h) |> filter(fn: (r) => r.user_id == \"user1\")"
    print(query)
    tables = query_api.query(query, org=organization)

    # the data will be returned as Python objects so you can iterate the data and do what you want
    for table in tables: 
        for record in table.records:
            print(record)

    # You can use the built in encoder to return results in json
    output = json.dumps(tables, cls=FluxStructureEncoder, indent=2)
    return output, 200

@app.route("/visualize")
def visualize():
    # create a graph and return it in html
    pass

@app.route("/alerts", methods=["POST, GET, DELETE"])
def alerts():
    # create, list, and delete alerts depending on method
    pass

def startup_checks():
    # check if the bucket is there, and if not, create it
    pass

if __name__ == '__main__':
    startup_checks()
    app.run(host='0.0.0.0', port=5001, debug=True)
