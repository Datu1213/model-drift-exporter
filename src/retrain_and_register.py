import os
import sys
import mlflow
import mlflow.sklearn
import ray
import json
import time
import requests
import pandas as pd
import warnings
from ray import tune, train
from ray.air.integrations.mlflow import MLflowLoggerCallback
from ray.tune.schedulers import ASHAScheduler

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression as SklearnLogisticRegression
from sklearn.metrics import accuracy_score

# --- 1. Global Config ---

# MLflow / MinIO 
MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
MLFLOW_S3_ENDPOINT_URL = "http://minio:9000"
AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"

# Ray 
RAY_ADDRESS = "ray://ray-head:10001"

# Spark
SPARK_MASTER_URL = "spark://spark-master:7077"
DBT_VIEW_NAME = "default.stg_user_events"

# Model Serving
SERVING_URL = "http://mlflow-model-server:5001/invocations"

# Ignore normal warnings of Ray Sklearn
warnings.filterwarnings("ignore", category=UserWarning, module="ray")
warnings.filterwarnings("ignore", category=FutureWarning, module="sklearn")


def setup_mlflow_environment():
    """ MLflow Client Envs"""
    print("--- 1. Set MLflow Client Envs ---")
    os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    print(f"MLFLOW_TRACKING_URI set to: {MLFLOW_TRACKING_URI}")

def setup_spark_session() -> SparkSession:
    """
    Create Spark Session
    It wil use /opt/spark/conf/spark-defaults.conf as Spark config file
    (Including Delta, Kafka, Hive pakages and config)
    """
    print("--- 2. Create Spark Session ---")
    try:
        spark = SparkSession.builder \
            .appName("MLOps_Pipeline_Script") \
            .master(SPARK_MASTER_URL) \
            .config("spark.hadoop.fs.s3a.endpoint", "minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .enableHiveSupport() \
            .getOrCreate()
            
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

        print(f"Spark Session (Version {spark.version}) successfully connected to {SPARK_MASTER_URL}.")
        return spark
    except Exception as e:
        print(f"Fatal Error: Can not create Spark Session: {e}")
        print("Please make suure Spark Master is running")
        sys.exit(1)

def train_sklearn_model(experiment_name: str, model_params = {"C": 0.5, "solver": "liblinear"}) -> tuple[str, str]:
    """
    Train a simple Sklearn model and record it using MLflow
    """
    print("--- 3. Start training (Sklearn) ---")
    mlflow.set_experiment(experiment_name)
    
    X, y = load_iris(return_X_y=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        print(f"Starting Sklearn run: {run_id}")
        
        model = SklearnLogisticRegression(**model_params)
        model.fit(X_train, y_train)
        accuracy = accuracy_score(y_test, model.predict(X_test))
        
        mlflow.log_params(model_params)
        mlflow.log_metric("accuracy", accuracy)
        
        # Prepare a input example to let it deduce signature
        input_example = pd.DataFrame(X_train[:5], columns=load_iris().feature_names)
        
        mlflow.sklearn.log_model(
            model, 
            "sklearn-model", # Artifacts Path
            input_example=input_example
        )
        print(f"Sklearn run {run_id} complete. Accuracy: {accuracy:.4f}")
        return run_id, "sklearn-model"

###### ------------------------- Deprecated ------------------------- ######
# def train_spark_model(spark: SparkSession, experiment_name: str) -> tuple[str, str]:
#     """
#     Train a Spark MLlib pipeline and record it using MLflow
#     """
    
#     print("--- 4. Start training (Spark MLlib) ---")
#     mlflow.set_experiment(experiment_name)
    
    
#     # a. Loading Data
#     try:
#         silver_df = spark.table(DBT_VIEW_NAME)
#         silver_df.cache()
#         print(f"Successfully loaded dbt view '{DBT_VIEW_NAME}'. Rows: {silver_df.count()}")
#     except Exception as e:
#         print(f"FAILED to load dbt view '{DBT_VIEW_NAME}'. Error: {e}")
#         return None, None

#     # b. Features engineering
#     features_df = silver_df.fillna(0.0, subset=['purchase_value'])
#     label_indexer = StringIndexer(inputCol="event_type", outputCol="label")
#     features_df = label_indexer.fit(features_df).transform(features_df)
    
#     categorical_cols = ["user_id", "page"]
#     indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_cols]
    
#     feature_cols = [f"{col}_index" for col in categorical_cols] + ["purchase_value"]
#     assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
#     # c. Define model and pipeline
#     lr = LogisticRegression(featuresCol="features", labelCol="label")
#     pipeline = Pipeline(stages=indexers + [assembler, lr])

#     # d. Train and trace
#     with mlflow.start_run() as run:
#         run_id = run.info.run_id
#         print(f"Starting Spark MLlib run: {run_id}")
        
#         print("Artifact URI:", mlflow.get_artifact_uri())
        
#         mlflow.log_param("model_type", "LogisticRegression (Spark MLlib)")
#         mlflow.log_param("feature_cols", ", ".join(feature_cols))
        
#         spark_model = pipeline.fit(features_df)

#         # Use transformed_df which contains colume like "*_index"
#         transformed_df = spark_model.transform(features_df)

#         input_example = transformed_df.select(feature_cols).limit(5).toPandas()
#         # input_example = mlflow.models.convert_input_example_to_serving_input(input_example)
#         print("Logging Spark ML Pipeline to MLflow...")
#         mlflow.spark.log_model(
#             spark_model, 
#             "spark-pipeline-model"
#         )
        
#         # (Evaluate modele...)
#         # ...
        
#         print(f"Spark MLlib run {run_id} complete.")
        
#         return run_id, "spark-pipeline-model"
###### ------------------------- Deprecated ------------------------- ######

def run_hyperparameter_tuning(experiment_name: str) -> dict:
    """
    Run Ray Tune HPO and record it using MLflow
    """
    print("--- 5. Start tuning (Ray Tune HPO) ---")
    RAY_ADDRESS = "ray://ray-head:10001"
    try:
        ray.init(address=RAY_ADDRESS)
        print(f"--- Ray Client: Successfully connect to Ray Head ({RAY_ADDRESS}) ---")
    except Exception as e:
        print(f"--- Ray Client: Connection fail: {e} ---")
        return None

    def train_objective(config: dict):
        """Ray Tune Target Function"""
        X, y = load_iris(return_X_y=True)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
        params = {"C": config["C"], "solver": config["solver"]}
        model = SklearnLogisticRegression(**params)
        model.fit(X_train, y_train)
        accuracy = accuracy_score(y_test, model.predict(X_test))
        # Use train.report() for ray 2.49.2
        tune.report({"accuracy": accuracy})

    # param_space
    search_space = {
        "C": tune.loguniform(1e-4, 1e-1),
        "solver": tune.choice(["liblinear", "saga", "lbfgs"])
    }

    # MLflowLoggerCallback
    mlflow_callback = MLflowLoggerCallback(
        tracking_uri=MLFLOW_TRACKING_URI,
        experiment_name=experiment_name,
        save_artifact=False # Don't save it to remote storage
    )
    
    # RunConfig
    run_config = train.RunConfig(
        name="hpo_logistic_regression",
        callbacks=[mlflow_callback],
        storage_path="/tmp/ray_results"
    )
    
    tune_config = tune.TuneConfig(
        metric="accuracy",
        mode="max",
        num_samples=20,
        scheduler=ASHAScheduler() # ASHA Scheduler
    )

    print("--- Start Ray Tune (Totally 20 times experiments) ---")
    
    tuner = tune.Tuner(
        train_objective,
        param_space=search_space,
        tune_config=tune_config,
        run_config=run_config,
    )
    
    results = tuner.fit()
    
    print("--- Fine tuning finished! ---")
    best_result = results.get_best_result(metric="accuracy", mode="max")
    
    best_params = best_result.config
    best_accuracy = best_result.metrics['accuracy']
    
    print(f"Best Accuracy: {best_accuracy:.4f}")
    print(f"Best Hyper-params: {best_params}")
    
    ray.shutdown()
    return best_params

def register_model(run_id: str, model_artifact_path: str, model_name: str, alias: str):
    """
    Register a model and assign it with an alias
    """
    print(f"--- 6. Register model: '{model_name}' ( Run ID: {run_id}) ---")
    model_uri = f"runs:/{run_id}/{model_artifact_path}"
    
    try:
        model_version = mlflow.register_model(
            model_uri=model_uri,
            name=model_name
        )
        print(f"Model  '{model_name}' successfully registered as Version {model_version.version}")
        time.sleep(5) # Wait til registration done
        
        # set_registered_model_alias
        client = mlflow.MlflowClient()
        client.set_registered_model_alias(
            name=model_name,
            version=model_version.version,
            alias=alias
        )
        print(f"Model Version: {model_version.version} successfully promotes to @'{alias}' ")
        
    except Exception as e:
        print(f"Model registration or promotion fail: {e}")

def call_model_server(model_name: str):
    """
    Call already deployed Model API。
    """
    print(f"--- 7. Call already deployed Model API: ({model_name}) ---")
    
    # (This test data is for 'iris_logistic_regression' (sklearn) )
    test_data = {
        "dataframe_split": {
            "columns": ["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"],
            "data": [
                [5.1, 3.5, 1.4, 0.2], # Expectation: 0
                [6.2, 3.4, 5.4, 2.3]  # Expectation: 2
            ]
        }
    }
    
    headers = {'Content-Type': 'application/json'}
    
    try:
        # (If 'mlflow-model-server' is serving models)
        response = requests.post(SERVING_URL, data=json.dumps(test_data), headers=headers)
        
        if response.status_code == 200:
            print("--- Successfully called Model Serving API ---")
            print(f"Status Code: {response.status_code}")
            print(f"Prediction Response: {response.json()}")
        else:
            print(f"--- Model Serving API Calling fail ---")
            print(f"Status Code: {response.status_code}")
            print(f"Response Body: {response.text}")
            
    except Exception as e:
        print(f"--- Model Serving API Calling fail Error ---")
        print(f"Can not connect {SERVING_URL}: {e}")

def main():
    """Run MLOps pipeline step by step"""
    
    # --- Part 1: Config ---
    setup_mlflow_environment()
    # spark = setup_spark_session()
    
    # --- Part 2: Sklearn train & register ---
    # sklearn_run_id, sklearn_model_path = train_sklearn_model(experiment_name="sklearn_experiment")
    # if sklearn_run_id:
    #     register_model(sklearn_run_id, sklearn_model_path, 
    #                    model_name="iris_logistic_regression", 
    #                    alias="Staging")


    # --- Part 4: HPO Fine tune ---
    best_params = run_hyperparameter_tuning(experiment_name="ray_tune_hpo_experiment")
    if best_params:
        train_sklearn_model(experiment_name="sklearn_experiment", model_params=best_params)
    # Retrain the model use baset params

    #--- Part 5: Test Model Serving ---
    # (If 'mlflow-model-server' is serving "iris_logistic_regression")
    call_model_server(model_name="iris_logistic_regression")

if __name__ == "__main__":
    main()