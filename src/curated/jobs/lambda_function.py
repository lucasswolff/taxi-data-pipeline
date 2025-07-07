## creates EMR cluster and run jobs. To be triggered once a month

import boto3

def lambda_handler(event, context):
    client = boto3.client('emr')

    env = event.get('ENV', 'dev')
    
    response = client.run_job_flow(
        Name="taxi-curated-job",
        ReleaseLabel="emr-7.9.0",
        Applications=[{"Name": "Spark"}],
        LogUri= f"s3://taxi-data-hub/logs/{env}/curated", # where logs will be stored
        Instances={
            "InstanceGroups": [
                {
                    "InstanceRole": "MASTER",
                    "InstanceCount": 1,
                    "InstanceType": "m5.xlarge"
                },
                {
                    "InstanceRole": "CORE",
                    "InstanceCount": 1,
                    "InstanceType": "m5.xlarge"
                }
            ],
            "KeepJobFlowAliveWhenNoSteps": False,  
            "TerminationProtected": False
        },
        # roles to assume
        JobFlowRole="EMR_EC2_DefaultRole",  
        ServiceRole="EMR_DefaultRole",
        # submittion of spark jobs
        Steps=[
            {
                "Name": "TaxiCurated",
                "ActionOnFailure": "TERMINATE_CLUSTER", 
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "--master", "yarn",
                        "--conf", "spark.sql.sources.partitionOverwriteMode=dynamic",
                        "--py-files", f"s3://taxi-data-hub/jobs_dependencies/{env}/curated.zip",
                        f"s3://taxi-data-hub/jobs/{env}/run_yellow_green_curate.py",
                        env,
                        "both",
                        "past_months",
                        "3"
                    ]
                }
            }
        ],
    Tags=[
        {
            "Key": "for-use-with-amazon-emr-managed-policies",
            "Value": "true"
        }
    ],
        VisibleToAllUsers=True
    )

    print("Cluster created:", response['JobFlowId'])
    return response
