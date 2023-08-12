import functions_framework
import base64
import json
from google.protobuf.timestamp_pb2 import Timestamp 
from datetime import datetime , timedelta
from google.cloud import bigquery_datatransfer_v1
from os import environ

def get_table_id(data_dict):

     print(data_dict)

     table_id = data_dict['protoPayload']['serviceData']\
                                         ['jobCompletedEvent']\
                                         ['job']\
                                         ['jobConfiguration']\
                                         ['query']['destinationTable']['tableId']
                                         #['load']\
                                         #['destinationTable']['tableId'] #En GA4

                                        
     return table_id

@functions_framework.cloud_event
def trigger_scheduled_query_to_transform(cloud_event):

     decoded_data = base64.b64decode(cloud_event.data['message']['data'])

     data_dict = json.loads(decoded_data)

     table_id = get_table_id(data_dict)

     # Grab the table date and turn it into the run time for the scheduled query
     tableTime = table_id.replace(environ["source_table_name"], '')  # envents_20230811 -> 20230811

     year = tableTime[:4]
     month = tableTime[4:6]
     day = tableTime[6:8]

     runTime = datetime( int(year), int(month) , int(day))

     requestedRuntime = Timestamp()

     requestedRuntime.FromDatetime(runTime)

     # Create the client
     client = bigquery_datatransfer_v1.DataTransferServiceClient()

     request = bigquery_datatransfer_v1.StartManualTransferRunsRequest()

     conn_string = environ['connection_string']

     # configurar la request
     
     request.parent = conn_string

     request.requested_run_time = requestedRuntime

     response = client.start_manual_transfer_runs(request) 
 
     return response
