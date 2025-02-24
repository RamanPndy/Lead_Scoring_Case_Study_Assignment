# Lead_Scoring_Case_Study_Assignment
Upgrad Lead Scoring Case Study Assignment

To Setup application MLFlow use mlflow_venv_requirements.txt for dependencies.
To Start MLFlow server
mlflow server --backend-store-uri='sqlite:///./02_training_pipeline/notebooks/lead_scoring_model_experimentation.db' --default-artifact-root="./mlruns" --port=6006 --host=0.0.0.0

To Setup application Airflow use airflow_venv_requirements.txt for dependencies.
To Start airflow
airflow db init
airflow users create --username raman --firstname raman --lastname pandey --role Admin --email raman.pndy@gmail.com --password raman@123
airflow webserver
airflow scheduler

To run notbooks use requirements.txt for dependencies.