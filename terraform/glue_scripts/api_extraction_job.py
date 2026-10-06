import json
import sys
from datetime import datetime
from urllib import response

import requests
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col


args = getResolvedOptions(sys.argv, 
                          ["JOB_NAME", 
                           "target_path", 
                            "api_url",
                            "api_start_date",
                            "api_end_date",
                            "max_rows",
                           ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def extract_data_from_api(api_url: str, max_rows: int):
    all_rows = []
    offset = 0
    page_size = 10000

    while len(all_rows) < max_rows:
        paged_url = f"{api_url}&$offset={offset}"
        response = requests.get(paged_url)
        if response.status_code != 200:
            print(f"Failed to fetch data from API. Status code: {response.status_code}")
            response.raise_for_status()
        page = response.json()
        if not page:
            break
        all_rows.extend(page)
        print(f"offset={offset} fetched={len(page)} total={len(all_rows)}")
        if len(page) < page_size:
            break
        offset += page_size

    return all_rows[:max_rows]

api_url = args["api_url"]
request_start_date = args["api_start_date"]
request_end_date = args["api_end_date"]
target_path = args["target_path"]
max_rows = int(args["max_rows"])
current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
print(f"Starting API extraction job at {current_timestamp} for date range {request_start_date} to {request_end_date}")


request_data = extract_data_from_api(api_url, max_rows)
api_data_df = spark.read.json(sc.parallelize([json.dumps(request_data)]))

api_data_df = api_data_df.coalesce(1)
api_data_df.write.mode("overwrite").json(f"{target_path}/api_data_{current_timestamp}/")

job.commit()