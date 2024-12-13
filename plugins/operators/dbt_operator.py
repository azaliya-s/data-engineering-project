import subprocess
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

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

    def execute(self, context):
        cmd = ["dbt", "run"]
        if self.models:
            cmd += ["--models", self.models]
        cmd += ["--profiles-dir", self.profiles_dir, "--project-dir", self.project_dir]
        self.log.info(f"Running DBT command: {' '.join(cmd)}")

        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                self.log.error(f"DBT run command failed with the following error:\n{stderr}")
                raise Exception(f"DBT run command failed: {stderr}")
            self.log.info(stdout)
        except subprocess.CalledProcessError as e:
            self.log.error(f"DBT run command failed with the following error:\n{e.stderr}")
            raise Exception(f"DBT run command failed: {e.stderr}")