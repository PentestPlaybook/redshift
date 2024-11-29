import psycopg2
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from psycopg2 import sql
import socket  # Used for nslookup equivalent
import subprocess  # Used for using 'nc' to check port connectivity
import re  # Used for regex parsing
import os  # Used to check if CSV file exists
import sys  # Used to exit the script
import json
import zipfile
import datetime  # Import datetime for proper handling in Lambda function

def parse_endpoint(endpoint):
    """
    Parse the endpoint string to extract host and port.
    """
    # Remove any leading or trailing whitespace
    endpoint = endpoint.strip()

    # Regular expression to match host, port, and optional path
    pattern = r'^(?P<host>[^:/]+(?:\.[^:/]+)*)(?::(?P<port>\d+))?(?:/(?P<path>.*))?$'

    match = re.match(pattern, endpoint)

    if match:
        host = match.group('host')
        port = match.group('port')

        if not port:
            port = "5439"  # Default port

        return host, port
    else:
        # If the endpoint doesn't match the pattern, return None
        return None, None

def extract_region_from_host(host):
    """
    Extract the AWS region from the Redshift cluster endpoint.
    """
    # Regex pattern to extract region
    pattern = r'.*\.(.*?)\.redshift\.amazonaws\.com$'
    match = re.match(pattern, host)
    if match:
        region = match.group(1)
        return region
    else:
        return None

def extract_cluster_identifier(host):
    """
    Extract the cluster identifier from the Redshift cluster endpoint.
    """
    # The cluster identifier is the first subdomain in the host
    parts = host.split('.')
    if len(parts) >= 1:
        cluster_identifier = parts[0]
        return cluster_identifier
    else:
        return None

def get_user_inputs():
    """
    Prompt the user for Redshift and S3 details.
    """
    while True:
        endpoint = input("Enter Redshift cluster endpoint (e.g., redshift-cluster-1.xxxx.us-west-2.redshift.amazonaws.com:5439/dev): ")

        host, port = parse_endpoint(endpoint)
        if not host:
            print("Invalid endpoint format. Please re-enter the endpoint.")
            continue

        # Perform nslookup equivalent
        try:
            socket.gethostbyname(host)
            print("Endpoint is valid.")
        except socket.error:
            print("Could not resolve host. Please re-enter the endpoint.")
            continue

        # Use 'nc' to try to connect to the endpoint and port
        try:
            result = subprocess.run(['nc', '-zv', host, port], capture_output=True, text=True)
            if result.returncode == 0:
                # Connection succeeded
                print("Port is open. Proceeding to the next step.")
                break
            else:
                # Connection failed
                print(f"Connection to port {port} failed. Please ensure your EC2 instance security group is added to the Redshift allow list for inbound port {port}")
        except Exception as e:
            print(f"An error occurred while trying to check the port: {e}")
            print(f"Connection failed. Please ensure your EC2 instance security group is added to the Redshift allow list for inbound port {port}")

    # Extract region from the host
    region = extract_region_from_host(host)
    if not region:
        print("Could not extract AWS region from the endpoint. Please ensure the endpoint is correct.")
        return None

    print(f"Region '{region}' extracted from the endpoint.")

    # Extract cluster identifier from the host
    cluster_identifier = extract_cluster_identifier(host)
    if not cluster_identifier:
        print("Could not extract cluster identifier from the endpoint. Please ensure the endpoint is correct.")
        return None

    print(f"Using '{cluster_identifier}' as cluster identifier.")

    # Validate cluster identifier
    try:
        client = boto3.client('redshift', region_name=region)
        response = client.describe_clusters(ClusterIdentifier=cluster_identifier)
        print("Cluster identifier is valid. Proceeding to the next step.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            print(f"Invalid cluster identifier '{cluster_identifier}'. Please check the endpoint and try again.")
            return None
        elif e.response['Error']['Code'] == 'AccessDenied':
            print(f"Access denied when trying to describe the cluster. Please check your IAM permissions.")
            return None
        else:
            print(f"An error occurred: {e}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    # Prompt for database username and check permissions
    while True:
        db_user = input("Enter the Redshift admin user name (e.g., 'admin_user'): ")
        # Attempt to get temporary credentials
        try:
            temp_username, temp_password = get_temporary_credentials(
                db_user=db_user,
                cluster_identifier=cluster_identifier,
                database='dev',  # Using 'dev' as a default database to test credentials
                region=region
            )
            # Attempt to connect to the 'dev' database using the temporary credentials
            try:
                conn_test = psycopg2.connect(
                    host=host,
                    port=port,
                    database='dev',
                    user=temp_username,
                    password=temp_password
                )
                conn_test.close()
                print("Database user is valid. Proceeding to the next step.")
                break
            except psycopg2.OperationalError as e:
                if 'does not exist' in str(e):
                    print(f"Database user '{db_user}' does not exist in the database. Please enter the Redshift admin user name.")
                else:
                    print(f"Error connecting to the database with user '{db_user}': {e}")
            except Exception as e:
                print(f"An error occurred while testing database connection: {e}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            if error_code == 'AccessDenied':
                print(f"User '{db_user}' does not have sufficient permissions. Please enter a user with appropriate permissions.")
            elif error_code == 'BadRequest':
                print(f"User '{db_user}' does not exist or is invalid. Error: {error_message}. Please enter a valid database user.")
            else:
                print(f"An error occurred: {error_message}")
        except Exception as e:
            print(f"An error occurred: {e}")

    # Prompt for database name and validate it
    while True:
        database = input("Enter Redshift database name (default is 'dev'. You can choose a different one or create a new database by entering its name): ") or "dev"
        try:
            # Get temporary credentials for the specified database
            temp_username, temp_password = get_temporary_credentials(
                db_user=db_user,
                cluster_identifier=cluster_identifier,
                database=database,
                region=region
            )
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=temp_username,
                password=temp_password
            )
            conn.close()
            print(f"Connected to Redshift database '{database}' successfully.")
            break
        except psycopg2.OperationalError as e:
            error_message = str(e)
            if 'does not exist' in error_message or 'database "' in error_message:
                create_db = input(f"Database '{database}' does not exist. Would you like to create a new database now? (yes/no): ").strip().lower()
                if create_db == 'yes':
                    try:
                        # Get temporary credentials for 'dev' database to create a new database
                        temp_username_dev, temp_password_dev = get_temporary_credentials(
                            db_user=db_user,
                            cluster_identifier=cluster_identifier,
                            database='dev',
                            region=region
                        )
                        conn = psycopg2.connect(
                            host=host,
                            port=port,
                            database='dev',
                            user=temp_username_dev,
                            password=temp_password_dev
                        )
                        conn.autocommit = True
                        cur = conn.cursor()
                        cur.execute(sql.SQL("CREATE DATABASE {}").format(quote_identifier(database)))
                        conn.close()
                        print(f"Database '{database}' created successfully.")
                        continue  # Go back to the beginning of the loop to try connecting again
                    except Exception as e:
                        print(f"Failed to create database '{database}': {e}")
                        continue
                else:
                    print("Please enter an existing database name.")
            else:
                print(f"Error connecting to the database: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    # Prompt for S3 file path and validate it
    while True:
        s3_path = input("Enter S3 file path (e.g., s3://your-bucket-name/your-file.csv): ")
        if s3_path.startswith("s3://"):
            print("S3 path is valid. Proceeding to the next step.")
            break
        else:
            print("Invalid S3 path. Please enter a valid S3 URI starting with 's3://'.")

    # Prompt for IAM Role ARN and validate it
    while True:
        iam_role = input("Enter IAM Role ARN for Redshift (e.g., arn:aws:iam::account-id:role/your-role): ")
        try:
            iam_client = boto3.client('iam')
            role_name = iam_role.split('/')[-1]
            response = iam_client.get_role(RoleName=role_name)
            print("IAM Role ARN is valid. Proceeding to the next step.")
            break
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchEntity':
                print(f"IAM Role '{iam_role}' does not exist. Please enter a valid IAM Role ARN.")
            elif error_code == 'AccessDenied':
                print(f"Access denied when trying to access the IAM Role. Please check your IAM permissions.")
            else:
                print(f"An error occurred: {e.response['Error']['Message']}")
        except Exception as e:
            print(f"An error occurred: {e}")

    # Prompt for schema name and validate it
    while True:
        schema_name = input("Enter the schema name (default is 'public'. You can choose a different one or create a new schema by entering its name): ") or "public"
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=temp_username,
                password=temp_password
            )
            cur = conn.cursor()
            # Check if the schema exists
            cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = %s;", (schema_name,))
            result = cur.fetchone()
            if result:
                print(f"Schema '{schema_name}' exists. Proceeding to the next step.")
                conn.close()
                break
            else:
                create_schema = input(f"Schema '{schema_name}' does not exist. Would you like to create a new schema now? (yes/no): ").strip().lower()
                if create_schema == 'yes':
                    try:
                        cur.execute(sql.SQL("CREATE SCHEMA {}").format(quote_identifier(schema_name)))
                        conn.commit()
                        print(f"Schema '{schema_name}' created successfully.")
                        conn.close()
                        break
                    except Exception as e:
                        conn.rollback()
                        print(f"Failed to create schema '{schema_name}': {e}")
                        conn.close()
                        continue
                else:
                    print("Please enter an existing schema name.")
                    conn.close()
        except Exception as e:
            print(f"An error occurred while validating or creating the schema: {e}")

    # Prompt for table name
    table_name = input("Enter the name of the Redshift table to create: ")

    # Prompt for local CSV path and validate it
    while True:
        local_csv_path = input("Enter the local path of the CSV for schema inference: ")
        if os.path.isfile(local_csv_path):
            print("Local CSV file found. Proceeding to the next step.")
            break
        else:
            print(f"File '{local_csv_path}' does not exist. Please enter a valid file path.")

    # Get AWS account ID
    sts_client = boto3.client('sts')
    account_id = sts_client.get_caller_identity()['Account']

    return {
        "host": host,
        "port": port,
        "database": database,
        "s3_path": s3_path,
        "iam_role": iam_role,
        "table_name": table_name,
        "schema_name": schema_name,
        "local_csv_path": local_csv_path,
        "region": region,
        "db_user": db_user,
        "cluster_identifier": cluster_identifier,
        "account_id": account_id
    }

def get_temporary_credentials(db_user, cluster_identifier, database, region):
    """
    Generate temporary database credentials using IAM role.
    """
    client = boto3.client('redshift', region_name=region)
    try:
        response = client.get_cluster_credentials(
            DbUser=db_user,
            DbName=database,
            ClusterIdentifier=cluster_identifier,
            AutoCreate=False,  # Set to False to prevent auto-creation of user
            DurationSeconds=3600  # Credentials valid for 1 hour
        )
        temp_username = response['DbUser']
        temp_password = response['DbPassword']
        return temp_username, temp_password
    except ClientError as e:
        print(f"Failed to get temporary credentials: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def map_data_types(df):
    """
    Map pandas dtypes to Redshift SQL data types.
    """
    dtype_mapping = {
        'object': 'VARCHAR(65535)',
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
    }
    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        col_type = dtype_mapping.get(dtype, 'VARCHAR(65535)')
        columns.append((col, col_type))
    return columns

def quote_identifier(identifier):
    """
    Safely quote SQL identifiers to handle special characters and reserved words.
    """
    return sql.Identifier(identifier)

def create_table_from_csv(csv_file_path, table_name, schema_name, conn):
    """
    Create a table in Redshift by inferring schema from the CSV file structure.
    """
    # Load the CSV locally to infer schema
    df = pd.read_csv(csv_file_path, nrows=1000)  # Read a sample to infer schema

    # Infer column names and types
    columns = map_data_types(df)

    # Construct the CREATE TABLE statement
    column_defs = [
        sql.SQL("{} {}").format(quote_identifier(col_name), sql.SQL(col_type))
        for col_name, col_type in columns
    ]
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            {}
        );
    """).format(
        quote_identifier(schema_name),
        quote_identifier(table_name),
        sql.SQL(', ').join(column_defs)
    )

    print("Executing CREATE TABLE query...")
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        print(f"Table {schema_name}.{table_name} created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Failed to create table: {e}")
        print("Did you enter the admin user name?")
        sys.exit(1)

def load_data_to_redshift(schema_name, table_name, s3_path, iam_role, region, conn):
    """
    Load data from S3 into the Redshift table using the COPY command.
    """
    copy_query = sql.SQL("""
        COPY {}.{}
        FROM %s
        IAM_ROLE %s
        REGION %s
        DELIMITER ','
        IGNOREHEADER 1
        FORMAT AS CSV
        EMPTYASNULL
        BLANKSASNULL
        TIMEFORMAT 'auto'
        TRUNCATECOLUMNS
        ACCEPTINVCHARS
        COMPUPDATE OFF
        STATUPDATE OFF;
    """).format(
        quote_identifier(schema_name),
        quote_identifier(table_name)
    )

    print("Executing COPY command...")
    try:
        with conn.cursor() as cur:
            cur.execute(copy_query, (s3_path, iam_role, region))
            conn.commit()
        print(f"Data loaded into table {schema_name}.{table_name} successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Failed to load data: {e}")
        raise

def generate_lambda_code(inputs):
    """
    Generate the Lambda function code as a string.
    """
    lambda_code = f'''import boto3
import json
import datetime

def lambda_handler(event, context):
    redshift = boto3.client('redshift-data', region_name='{inputs['region']}')

    sql_statement = """
        COPY {inputs['schema_name']}.{inputs['table_name']}
        FROM '{inputs['s3_path']}'
        IAM_ROLE '{inputs['iam_role']}'
        REGION '{inputs['region']}'
        DELIMITER ','
        IGNOREHEADER 1
        FORMAT AS CSV
        EMPTYASNULL
        BLANKSASNULL
        TIMEFORMAT 'auto'
        TRUNCATECOLUMNS
        ACCEPTINVCHARS
        COMPUPDATE OFF
        STATUPDATE OFF;
    """

    try:
        response = redshift.execute_statement(
            ClusterIdentifier='{inputs['cluster_identifier']}',
            Database='{inputs['database']}',
            DbUser='{inputs['db_user']}',
            Sql=sql_statement
        )
        print("Data load initiated successfully.")
        # Process the response to ensure JSON serializable
        serializable_response = make_serializable(response)
        return serializable_response
    except Exception as e:
        print(f"Error executing COPY command: {{e}}")
        raise

def make_serializable(obj):
    """
    Recursively convert objects to make them JSON serializable.
    """
    if isinstance(obj, dict):
        return {{k: make_serializable(v) for k, v in obj.items()}}
    elif isinstance(obj, list):
        return [make_serializable(element) for element in obj]
    elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    else:
        return obj
'''
    return lambda_code

def create_lambda_iam_role(role_name, inputs):
    """
    Create an IAM role for the Lambda function if it doesn't exist and attach necessary policies.
    """
    iam_client = boto3.client('iam')
    lambda_assume_role_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Effect': 'Allow',
                'Principal': {
                    'Service': 'lambda.amazonaws.com'
                },
                'Action': 'sts:AssumeRole'
            }
        ]
    }

    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(lambda_assume_role_policy),
            Description='Role for Lambda function to load data into Redshift'
        )
        iam_role_arn = response['Role']['Arn']
        print(f"IAM role {role_name} created.")
    except iam_client.exceptions.EntityAlreadyExistsException:
        response = iam_client.get_role(RoleName=role_name)
        iam_role_arn = response['Role']['Arn']
        print(f"IAM role {role_name} already exists.")

        # Get the existing assume role policy document
        assume_role_policy = response['Role']['AssumeRolePolicyDocument']

        # Check if the policy already allows Lambda to assume the role
        lambda_principal_exists = False
        for statement in assume_role_policy.get('Statement', []):
            principal_service = statement.get('Principal', {}).get('Service', [])
            if isinstance(principal_service, str):
                principal_service = [principal_service]
            if 'lambda.amazonaws.com' in principal_service:
                lambda_principal_exists = True
                break

        if not lambda_principal_exists:
            # Add the necessary statement to the assume role policy
            assume_role_policy['Statement'].append({
                'Effect': 'Allow',
                'Principal': {
                    'Service': 'lambda.amazonaws.com'
                },
                'Action': 'sts:AssumeRole'
            })
            # Update the assume role policy document
            iam_client.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=json.dumps(assume_role_policy)
            )
            print(f"Updated assume role policy for {role_name} to allow Lambda service.")
        else:
            print(f"Assume role policy for {role_name} already allows Lambda service.")

    # Attach necessary managed policies to the role
    managed_policies = [
        'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
        'arn:aws:iam::aws:policy/AmazonRedshiftDataFullAccess',
        'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    ]
    for policy_arn in managed_policies:
        try:
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            print(f"Attached policy {policy_arn} to role {role_name}.")
        except iam_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                continue
            else:
                print(f"Error attaching policy {policy_arn}: {e}")
                raise

    # Create inline policy for redshift:GetClusterCredentials and include cluster resource
    inline_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Effect': 'Allow',
                'Action': 'redshift:GetClusterCredentials',
                'Resource': [
                    f"arn:aws:redshift:{inputs['region']}:{inputs['account_id']}:cluster:{inputs['cluster_identifier']}",
                    f"arn:aws:redshift:{inputs['region']}:{inputs['account_id']}:dbname:{inputs['cluster_identifier']}/{inputs['database']}",
                    f"arn:aws:redshift:{inputs['region']}:{inputs['account_id']}:dbuser:{inputs['cluster_identifier']}/{inputs['db_user']}"
                ]
            }
        ]
    }

    try:
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName='RedshiftGetClusterCredentialsPolicy',
            PolicyDocument=json.dumps(inline_policy)
        )
        print(f"Attached inline policy 'RedshiftGetClusterCredentialsPolicy' to role {role_name}.")
    except Exception as e:
        print(f"Error attaching inline policy: {e}")
        raise

    return iam_role_arn

def create_lambda_function(inputs, lambda_filename):
    """
    Create the Lambda function in AWS.
    """
    lambda_client = boto3.client('lambda', region_name=inputs['region'])

    # Zip the lambda function code
    zip_filename = 'redshift_load_lambda.zip'
    with zipfile.ZipFile(zip_filename, 'w') as z:
        z.write(lambda_filename)
    print(f"Lambda function code zipped into {zip_filename}.")

    # Create IAM role for Lambda if not exists
    iam_role_name = 'RedshiftLoadLambdaRole'
    iam_role_arn = create_lambda_iam_role(iam_role_name, inputs)

    # Read the zip file contents
    with open(zip_filename, 'rb') as f:
        zipped_code = f.read()

    try:
        response = lambda_client.create_function(
            FunctionName='RedshiftLoadLambdaFunction',
            Runtime='python3.8',
            Role=iam_role_arn,
            Handler='redshift_load_lambda.lambda_handler',
            Code={'ZipFile': zipped_code},
            Description='Lambda function to load data from S3 to Redshift',
            Timeout=300,
            MemorySize=128,
            Publish=True
        )
        print("Lambda function created successfully.")
    except lambda_client.exceptions.ResourceConflictException:
        # Function already exists, update it
        try:
            response = lambda_client.update_function_code(
                FunctionName='RedshiftLoadLambdaFunction',
                ZipFile=zipped_code,
                Publish=True
            )
            print("Lambda function code updated successfully.")
        except Exception as e:
            print(f"Error updating Lambda function code: {e}")
            raise
    except Exception as e:
        print(f"Error creating/updating Lambda function: {e}")
        raise

def setup_eventbridge_rule(inputs, frequency):
    """
    Set up an EventBridge rule to trigger the Lambda function at the specified frequency.
    """
    events_client = boto3.client('events', region_name=inputs['region'])
    lambda_client = boto3.client('lambda', region_name=inputs['region'])

    # Define the schedule expression based on the frequency
    if frequency == 'daily':
        schedule_expression = 'rate(1 day)'
    elif frequency == 'weekly':
        schedule_expression = 'rate(7 days)'
    elif frequency == 'monthly':
        schedule_expression = 'cron(0 0 1 * ? *)'  # At 00:00 on day-of-month 1
    else:
        print("Invalid frequency.")
        return

    rule_name = 'RedshiftLoadLambdaSchedule'

    # Create or update the EventBridge rule
    try:
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            State='ENABLED',
            Description='Schedule to trigger the Lambda function to load data into Redshift'
        )
        rule_arn = response['RuleArn']
        print("EventBridge rule created or updated successfully.")
    except Exception as e:
        print(f"Error creating EventBridge rule: {e}")
        raise

    # Add Lambda function as target
    try:
        # Get Lambda function ARN
        response = lambda_client.get_function(FunctionName='RedshiftLoadLambdaFunction')
        lambda_function_arn = response['Configuration']['FunctionArn']

        # Add Lambda function as target to the EventBridge rule
        response = events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': lambda_function_arn
                }
            ]
        )
        print("Lambda function added as target to EventBridge rule.")

        # Give permission to EventBridge to invoke the Lambda function
        try:
            lambda_client.add_permission(
                FunctionName='RedshiftLoadLambdaFunction',
                StatementId='EventBridgeInvokeLambda',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=rule_arn
            )
            print("Permission granted to EventBridge to invoke the Lambda function.")
        except lambda_client.exceptions.ResourceConflictException:
            # Permission already exists
            print("Permission for EventBridge to invoke Lambda function already exists.")
    except Exception as e:
        print(f"Error adding target to EventBridge rule: {e}")
        raise

def schedule_automatic_load(inputs):
    # Ask for frequency
    frequency = input("Enter the frequency of automatic load (Daily, Weekly, Monthly): ").strip().lower()
    # Validate frequency
    if frequency not in ['daily', 'weekly', 'monthly']:
        print("Invalid frequency. Please enter 'Daily', 'Weekly', or 'Monthly'.")
        return

    # Generate the Lambda function code
    lambda_code = generate_lambda_code(inputs)

    # Save the Lambda function code as a .py file
    lambda_filename = 'redshift_load_lambda.py'
    with open(lambda_filename, 'w') as f:
        f.write(lambda_code)
    print(f"Lambda function code saved to {lambda_filename}")

    # Create the Lambda function in AWS
    create_lambda_function(inputs, lambda_filename)

    # Set up EventBridge rule
    setup_eventbridge_rule(inputs, frequency)
    print("Automatic load scheduled successfully.")

def main():
    """
    Main function to prompt user inputs, create a table, and load data into Redshift.
    """
    conn = None
    try:
        # Prompt for inputs
        inputs = get_user_inputs()
        if inputs is None:
            print("Failed to get necessary inputs. Exiting.")
            return

        # Get temporary credentials
        temp_username, temp_password = get_temporary_credentials(
            db_user=inputs["db_user"],
            cluster_identifier=inputs["cluster_identifier"],
            database=inputs["database"],
            region=inputs["region"]
        )

        # Connect to Redshift using temporary credentials
        conn = psycopg2.connect(
            host=inputs["host"],
            port=inputs["port"],
            database=inputs["database"],
            user=temp_username,
            password=temp_password
        )
        print("Connected to Redshift using IAM authentication")

        # Step 1: Create the table
        print("Inferring schema and creating table...")
        create_table_from_csv(
            inputs["local_csv_path"],
            inputs["table_name"],
            inputs["schema_name"],
            conn
        )

        # Step 2: Load the data
        print("Loading data into Redshift...")
        load_data_to_redshift(
            inputs["schema_name"],
            inputs["table_name"],
            inputs["s3_path"],
            inputs["iam_role"],
            inputs["region"],
            conn
        )

        # After loading data, ask if the user wants to schedule automatic load
        schedule_choice = input(f"Would you like to schedule an automatic load from {inputs['s3_path']}? (yes/no): ").strip().lower()
        if schedule_choice == 'yes':
            schedule_automatic_load(inputs)
        else:
            print("Data load complete. No automatic scheduling set up.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
