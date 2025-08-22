import snowflake.connector
from dotenv import load_dotenv
import os

from simple_salesforce import Salesforce
from simple_salesforce.bulk import SFBulkHandler

# Loading the environment variables
load_dotenv()

def loadDataFromSnowflake():

    # Create a connection to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('sf_user'),
        password=os.getenv('sf_password'),
        account=os.getenv('sf_account'),
        warehouse=os.getenv('sf_warehouse'),
        database=os.getenv('sf_database'),
        schema=os.getenv('sf_schema')
    )

    # cursor object to execute queries
    cur = conn.cursor()

    query = """
        Select * from Students
    """
    cur.execute(query)

    # Fetching the results of the query
    data = cur.fetchall()

    # printing data to verify that the data has been loaded from snowflake successfully
    for sampleData in data:
        print(f"ID = {sampleData[0]} , Name = {sampleData[1]} , Age = {sampleData[2]} , Height = {sampleData[3]}")

    # closing the cursor and connections
    cur.close()
    conn.close()

    return data


def dumpDataIntoSalesforce(data):

    # salesforce authentication
    sf = Salesforce(username=os.getenv("SF_username") , password=os.getenv("SF_password") , security_token=os.getenv("SF_securitytoken"))

    if sf.session_id and sf.instance_url:
        print("Salesforce connection successful.")
    else:
        print("Salesforce connection failed.")

    # creating an instance of SFBulkHandler
    bulkHandler = SFBulkHandler(session_id=sf.session_id , bulk_url=sf.bulk_url)

    if bulkHandler.bulk_url:
        print(f"Successfully connected to Salesforce Bulk API: {bulkHandler.bulk_url}")
    else:
        print("Failed to connect to Bulk API.")

    # < ----------------- CODE THAT USES SFBULKHANDLER TO INSERT THE STUDENT DATA INTO SALESFORCE CUSTOM STUDENT OBJECT USING BULK APIS ---------------------------------------- >

    # Transform Snowflake data into a format compatible with the Student__c object
    studentData = [
        {
            "StudentID__c": int(sampleData[0]),
            "StudentName__c": sampleData[1],
            "StudentAge__c": int(sampleData[2]), 
            "StudentHeight__c": float(sampleData[3])
        }
        for sampleData in data
    ]

    # Upserting the data into the salesforce via BulkHandler
    bulkHandler.submit_dml(object_name="Student__c", dml="upsert",data=studentData, external_id_field="StudentID__c")


def runPipeline():

    data = loadDataFromSnowflake()
    dumpDataIntoSalesforce(data)


if __name__ == "__main__":
    runPipeline()