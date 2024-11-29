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
        "cluster_identifier": cluster_identifier
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

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
