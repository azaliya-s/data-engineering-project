from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash import BashOperator
import os

# class DbtRunOperator(BaseOperator):
#     @apply_defaults
#     def __init__(
#         self,
#         models=None,
#         profiles_dir="/usr/local/airflow/dbt",
#         project_dir="/usr/local/airflow/dbt",
#         *args, 
#         **kwargs
#     ):
#         super().__init__(*args, **kwargs)
#         self.models = models
#         self.profiles_dir = profiles_dir
#         self.project_dir = project_dir

#     def execute(self, context):
#         cmd = ["dbt", "run"]
#         if self.models:
#             cmd += ["--models", self.models]
#         cmd += ["--profiles-dir", self.profiles_dir, "--project-dir", self.project_dir]

#         self.log.info(f"Running DBT command: {' '.join(cmd)}")
#         process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#         stdout, stderr = process.communicate()

#         if process.returncode != 0:
#             self.log.error(stderr.decode("utf-8"))
#             raise Exception(f"DBT run command failed: {stderr.decode('utf-8')}")
#         self.log.info(stdout.decode("utf-8"))

class DbtRunOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        models=None,
        profiles_dir="/usr/local/airflow/dbt",
        project_dir="/usr/local/airflow/dbt",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.models = models
        self.profiles_dir = profiles_dir
        self.project_dir = project_dir

    def get_bash_command(self):
        cmd = ["dbt", "run"]
        if self.models:
            cmd += ["--models", self.models]
        cmd += ["--profiles-dir", self.profiles_dir, "--project-dir", self.project_dir]
        # Превращаем список в строку
        return " ".join(cmd)

    def execute(self, context):
        bash_command = self.get_bash_command()
        self.log.info(f"Running DBT command: {bash_command}")

        dag = context['dag']
        bash_op = BashOperator(
            task_id=self.task_id,
            bash_command=bash_command,
            dag=dag
        )

        return bash_op.execute(context)


class DbtTestOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        profiles_dir="/usr/local/airflow/dbt",
        project_dir="/usr/local/airflow/dbt",
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.profiles_dir = profiles_dir
        self.project_dir = project_dir

    def get_bash_command(self):
        cmd = [
            "dbt", "test",
            "--profiles-dir", self.profiles_dir,
            "--project-dir", self.project_dir
        ]
        return " ".join(cmd)

    def execute(self, context):
        bash_command = self.get_bash_command()
        self.log.info(f"Running DBT command: {bash_command}")

        dag = context['dag']
        bash_op = BashOperator(
            task_id=self.task_id,
            bash_command=bash_command,
            dag=dag
        )

        return bash_op.execute(context)
