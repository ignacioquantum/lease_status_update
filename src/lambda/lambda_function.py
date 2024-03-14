import boto3

def lambda_handler(event, context):
    # Nombre del job de Glue a triggerear
    job_name = 'quantum_lease-status_updater_leaseparameters'

    # Cliente de Glue
    glue_client = boto3.client('glue')

    try:
        # Triggerear el job de Glue
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"Se ha iniciado el job de Glue '{job_name}' con JobRunId: {job_run_id}")
        return {
            'statusCode': 200,
            'body': f"Job de Glue '{job_name}' iniciado con JobRunId: {job_run_id}"
        }
    except Exception as e:
        print(f"Error al iniciar el job de Glue '{job_name}': {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error al iniciar el job de Glue '{job_name}': {str(e)}"
        }


# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/


