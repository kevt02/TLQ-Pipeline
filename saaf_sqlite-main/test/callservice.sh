#!/bin/bash

# JSON object to pass to Lambda Function

json='{
  "bucketname": "records-462",
  "filename": "100SalesRecords.csv",
  "filters": {
    "Region": "Asia"
  },
  "aggregations": ["avg(OrderProcessingTime)", "avg(GrossMargin)"]
}'

#echo "Invoking Lambda function using API Gateway"

#time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://lfbckqyqta.execute-api.us-east-2.amazonaws.com/processWithARM`

#echo ""



#echo ""

#echo "JSON RESULT:"

#echo $output | jq

#echo ""




echo "Invoking Lambda function using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name queryRecords --region us-east-2 --payload "$json" /dev/stdout | head -n 1 | head -c -2 ; echo`

echo ""

echo "JSON RESULT:"

echo $output | jq

echo ""






