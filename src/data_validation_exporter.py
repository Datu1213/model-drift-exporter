from pyspark.sql import SparkSession

import datetime
import sys
import json

import great_expectations as gx
from great_expectations.exceptions import DataContextError
from great_expectations.checkpoint import UpdateDataDocsAction

# ==============================================================================
# Step 0: Initialize Spark Session
# ==============================================================================

try:
  print("Spark Session not found, creating a new one...")
  spark = SparkSession.builder \
        .appName("Data Valition") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "2") \
        .enableHiveSupport() \
        .getOrCreate()
  print("New Spark Session created.")
except Exception as e:
  print(f"Error initializing Spark Session: {e}")
  # In environments like Jupyter/Databricks, getting the session might differ
  # Exit here as Spark Session is required
  sys.exit("Spark Session is mandatory.")


# ==============================================================================
# Step 1: Get Great Expectations Context
# ==============================================================================
# Use FileDataContext to persist configuration, expectations, and validation results to disk
# This is crucial for generating Data Docs
print("Step 1: Getting Great Expectations Context...")
project_root_dir = "/opt/ge_spark_project"
try:
    context = gx.get_context(mode="file", project_root_dir=project_root_dir)
    print(f"Successfully got or created FileDataContext, project root: {project_root_dir}")
except Exception as e:
    print(f"Error getting Context: {e}")
    sys.exit("Could not initialize GE Context.")

# ==============================================================================
# Step 2: Get or Update Data Source
# ==============================================================================
print("\nStep 2: Getting or Updating Data Source...")
data_source_name = "my_spark_delta_source" # Renamed for clarity
try:
    data_source = context.data_sources.add_or_update_spark(name=data_source_name)
    print(f"Successfully got/updated Spark Data Source: {data_source_name}")
except Exception as e:
    print(f"Error adding/updating Data Source: {e}")
    sys.exit("Could not set up Data Source.")

# ==============================================================================
# Step 3: Get or Update Data Asset
# ==============================================================================
print("\nStep 3: Getting or Updating Data Asset...")
# This Asset represents a "slot" where we will put our DataFrame
data_asset_name = "my_delta_dataframe_asset" # Renamed for clarity
try:
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)
    print(f"Successfully got/updated DataFrame Asset: {data_asset_name}")
except Exception as e:
    print(f"Error adding/updating Data Asset: {e}")
    sys.exit("Could not set up Data Asset.")

# ==============================================================================
# Step 4: Add Batch Definition to Asset
# ==============================================================================
print("\nStep 4: Adding Batch Definition to Asset...")
# This definition tells GE that we will provide a full DataFrame as a batch
batch_definition_name = "my_whole_dataframe_definition" # Renamed for clarity
try:
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )
    print(f"Successfully added Batch Definition: {batch_definition_name}")
except Exception as e:
    print(f"Error adding Batch Definition: {e}")
    sys.exit("Could not set up Batch Definition.")

# ==============================================================================
# Step 5: Read Delta Lake Data from Spark
# ==============================================================================
print("\nStep 5: Reading Delta Lake Data from Spark...")
table_name = "default.stg_user_events"
try:
    # Note: .option("versionAsOf", 1041) is hardcoded. Remove this line if you need the latest data.
    df =  spark.table("default.stg_user_events")
    # df = spark.read.format("delta").option("versionAsOf", 1041).load(full_path)
    
    print(f"Successfully loaded Delta Lake data from {table_name}.")
    print("Data Schema:")
    df.printSchema()
except Exception as e:
    print(f"Error reading Delta Lake data: {e}")
    sys.exit("Could not load source data.")

# ==============================================================================
# Step 6: Create Batch Parameters
# ==============================================================================
print("\nStep 6: Creating Batch Parameters...")
# This is the key to passing our DataFrame to the Batch Definition
batch_parameters = {"dataframe": df}
print("Batch Parameters are ready.")

# ==============================================================================
# Step 7: Define and Add Expectation Suite
# ==============================================================================
print("\nStep 7: Defining and Adding Expectation Suite...")
suite_name = "user_events_delta_suite"
try:
    # Create the suite object
    suite = gx.ExpectationSuite(name=suite_name)

    # Add expectations to the suite object
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="user_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"))
    
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="event_type"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="event_type", 
        value_set=['click', 'page_view', 'purchase', 'add_to_cart']
    ))

    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="page"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="page", 
        value_set=['/home', '/products/1', '/products/2', '/cart', '/checkout']
    ))
    
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="ts"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="ts"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="ts", type_="TimestampType"))

    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="purchase_value"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="purchase_value", 
        min_value=5.0, 
        max_value=100.0,
        mostly=0.95, # Allow a few nulls or invalid values (e.g., for non-purchase events)
        # row_condition='col("event_type") == "purchase"'  # Custom condition to allow nulls
    ))

    # Save (or update) the configured suite to the Context
    context.suites.add_or_update(suite)
    print(f"Successfully defined and saved Expectation Suite: {suite_name}")
except Exception as e:
    print(f"Error processing Expectation Suite: {e}")
    sys.exit("Could not create Expectation Suite.")

# ==============================================================================
# Step 8: Define and Add Validation Definition
# ==============================================================================
print("\nStep 8: Defining and Adding Validation Definition...")
validation_definition_name = "validate_my_delta_dataframe"
try:
    validation_definition = gx.core.validation_definition.ValidationDefinition(
        name=validation_definition_name, 
        data=batch_definition,  # Tell it which data definition to use
        suite=suite             # Tell it which expectation suite to use
    )
    # Save the validation definition to the Context
    context.validation_definitions.add_or_update(validation_definition)
    print(f"Successfully defined and saved Validation Definition: {validation_definition_name}")
except Exception as e:
    print(f"Error defining Validation Definition: {e}")
    sys.exit("Could not create Validation Definition.")

# ==============================================================================
# Step 9 & 10: Define Data Docs Site and Actions
# ==============================================================================
print("\nStep 9 & 10: Configuring Data Docs Site and Actions...")
site_name = "my_data_docs_site"
base_directory = "uncommitted/validations/" # Relative to project_root_dir

bucket = "data-engineering"
prefix = "gx/data_docs"
aws_access_key_id = "minioadmin"
aws_secret_access_key = "minioadmin"
minio_endpoint_url = "http://minio:9000"


try:
    # Define Site configuration (if needed)
    # Note: GE v1.x often handles a default site automatically, but explicit is clearer
    site_config = {
        "class_name": "SiteBuilder",
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        "store_backend": {
            "class_name": "TupleS3StoreBackend",   # Do not change it
            "bucket": bucket,
            "prefix": prefix,              # Storage Path Prefix
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key":aws_secret_access_key,
            "endpoint_url": minio_endpoint_url,  # MinIO URL
        }
    }
    # context.add_data_docs_site(site_name=site_name, site_config=site_config) # Use on first run
    context.update_data_docs_site(site_name=site_name, site_config=site_config) # Use on subsequent runs
    print(f"Successfully configured Data Docs Site: {site_name}")

    # Define Actions to run after the Checkpoint
    actions = [
        UpdateDataDocsAction(name="update_all_data_docs", site_names=[site_name])
    ]
    print("Successfully defined UpdateDataDocsAction.")

except Exception as e:
    print(f"Error configuring Data Docs Site or Actions: {e}")
    # We can still run the validation, but docs might not update
    actions = [] # Clear actions to prevent failure

# ==============================================================================
# Step 11: Define Checkpoint
# ==============================================================================
print("\nStep 11: Defining and Adding Checkpoint...")
checkpoint_name = "my_delta_dataframe_checkpoint"
try:
    checkpoint = gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=[validation_definition], # Use our definition from Step 8
        actions=actions,                               # Use our Action from Step 10
        result_format={"result_format": "BOOLEAN_ONLY"}  # Get detailed results
    )
    # Save the Checkpoint to the Context
    context.checkpoints.add_or_update(checkpoint)
    print(f"Successfully defined and saved Checkpoint: {checkpoint_name}")
except Exception as e:
    print(f"Error defining Checkpoint: {e}")
    sys.exit("Could not create Checkpoint.")

# ==============================================================================
# Step 12 & 13: Run Checkpoint and View Results
# ==============================================================================
print("\nStep 12 & 13: Running Checkpoint...")
try:
    # Define a unique Run ID
    run_id = gx.core.run_identifier.RunIdentifier(
        run_name=f"delta_validation_run_{datetime.datetime.now():%Y%m%d_%H%M%S}",
        run_time=datetime.datetime.now()
    )
    print(f"Using Run ID: {run_id.run_name}")

    # Run the Checkpoint!
    # We pass the batch_parameters defined in Step 6 here
    validation_results = checkpoint.run(
        batch_parameters=batch_parameters, 
        run_id=run_id
    )
                                                                                                                                            
    print(json.loads(validation_results.describe()).get("success") == True)
    # print(f"Success: {json.load(validation_results.describe()).get('success', False) == True}")
    print("\n================ Validation Results ================")
    print(validation_results)
    print("====================================================")

    # (Optional) Automatically build and open data docs
    # print("\nBuilding and opening Data Docs...")
    # context.build_data_docs(site_names=[site_name])
    # context.open_data_docs(site_name=site_name) # This may not open a browser in some environments

    print(f"\nValidation complete. Check the Data Docs here:")
    print(f"s3://{bucket}/{prefix}/index.html")

    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
    registry = CollectorRegistry()
    g_drift_score = Gauge(
        'data_valition_result', 
        'Data Validation', 
        ['table_name'], 
        registry=registry
    )
    data_validation_result = 1 if json.loads(validation_results.describe()).get("success") else 0
    table_name = "stg_user_events_v1"
    g_drift_score.labels(table_name=table_name).set(data_validation_result)
    push_to_gateway('pushgateway:9091', job='gx_data_validation', registry=registry)
except Exception as e:
    print(f"Error running Checkpoint: {e}")
    sys.exit("Checkpoint run failed.")

