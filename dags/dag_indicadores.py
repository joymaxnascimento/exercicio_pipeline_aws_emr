from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
cluster_id = Variable.get('cluster_id')
python_file_job = Variable.get('python_file_job')

client = boto3.client(
    'emr'
    , region_name='us-east-1'
    , aws_access_key_id=aws_access_key_id
    , aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': 'Joymax/Wagner',
    'start_date': datetime(2022, 4, 2)
}

@dag(default_args=default_args, schedule_interval='*/5 * * * *', description='Executa um job Spark no EMR.', catchup=False, tags=['spark','emr'])
def indicadores():

    @task
    def inicio():
        return True
    
    @task
    def emr_process_market_basket(success_before: bool):
        print(cluster_id)
        print(python_file_job)
        if success_before:
            newstep = client.add_job_flow_steps(
                JobFlowId=cluster_id,
                Steps=[{
                    'Name': 'Processa indicadores',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 python_file_job
                                 ]
                    }
                }]
            )
            return newstep['StepIds'][0]

    @task
    def wait_emr_job(step_id: str):
        waiter = client.get_waiter('step_complete')

        waiter.wait(
            ClusterId=cluster_id,
            StepId=step_id,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 120
            }
        )
        return True

    fim = DummyOperator(task_id="fim")

    # Orquestração
    start = inicio()
    indicadores = emr_process_market_basket(start)
    wait_step = wait_emr_job(indicadores)
    wait_step >> fim
    #---------------

execucao = indicadores()
