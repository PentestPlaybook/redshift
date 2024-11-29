import psycopg2
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from psycopg2 import sql
import socket  # Used for nslookup equivalent
import subprocess  # Used for using 'nc' to check port connectivity
import re  # Used for regex parsing

def get_user_inputs():
    """
    Prompt the user for Redshift and S3 details.
    """
    while True:
        host = input("Enter Redshift cluster endpoint (e.g., redshift-cluster-1.xxxx.us-west-2.redshift.amazonaws.com): ")
        # Perform nslookup equivalent
        try:
            socket.gethostbyname(host)
            # If successful, break out of the loop
            print("Endpoint is valid. Proceeding to the next step.")
            break
        except socket.error:
            print("Connection failed. Please re-enter the endpoint to try again.")

    # Extract region from the endpoint
    region = extract_region_from_host(host)
    if not region:
        print("Could not extract AWS region from the endpoint. Please ensure the endpoint is correct.")
        return None

    print(f"Region '{region}' extracted from the endpoint.")

    while True:
        port = input("Enter Redshift port (default: 5439): ") or "5439"
        # Use 'nc' to try to connect to the endpoint and port
        try:
            result = subprocess.run(['nc', '-zv', host, port], capture_output=True, text=True)
            if result.returncode == 0:
                # Connection succeeded
                print("Port is open. Proceeding to the next step.")
                break
            else:
                # Connection failed
                print(f"Connection failed. Please ensure your EC2 instance security group is added to the Redshift allow list for inbound port {port}")
        except Exception as e:
            print(f"An error occurred while trying to check the port: {e}")
            print(f"Connection failed. Please ensure your EC2 instance security group is added to the Redshift allow list for inbound port {port}")

    # Prompt for cluster identifier and validate it
    while True:
        cluster_identifier = input("Enter your Redshift cluster identifier: ")
        try:
            client = boto3.client('redshift', region_name=region)
            response = client.describe_clusters(ClusterIdentifier=cluster_identifier)
            print("Cluster identifier is valid. Proceeding to the next step.")
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print(f"Invalid cluster identifier '{cluster_identifier}'. Please enter a valid cluster identifier.")
            elif e.response['Error']['Code'] == 'AccessDenied':
                print(f"Access denied when trying to describe the cluster. Please check your IAM permissions.")
                continue
            else:
                print(f"An error occurred: {e}")
                continue
        except Exception as e:
            print(f"An error occurred: {e}")
            continue

    # Prompt for database username and check permissions
    while True:
        db_user = input("Enter a database user name (this can be any string, e.g., 'iam_user'): ")
        # Attempt to get temporary credentials
        try:
            temp_username, temp_password = get_temporary_credentials(
                db_user=db_user,
                cluster_identifier=cluster_identifier,
                database='dev',  # Using 'dev' as a default database to test credentials
                region=region
            )
            # Attempt to connect to the database using temp credentials
            try:
                test_conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database='dev',  # Adjust if 'dev' is not the default database
                    user=temp_username,
                    password=temp_password
                )
                test_conn.close()
                print("Database user is valid. Proceeding to the next step.")
                break
            except psycopg2.Error as e:
                print(f"Failed to connect to the database with user '{db_user}': {e}")
                print(f"User '{db_user}' is invalid or does not have access to the database.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                print(f"User '{db_user}' does not have sufficient permissions. Please enter a superuser name.")
            else:
                print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    # Prompt for database name and validate it
    while True:
        database = input("Enter Redshift database name: ")
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=temp_username,
                password=temp_password
            )
            conn.close()
            print("Connected to Redshift database successfully.")
            break
        except psycopg2.OperationalError as e:
            if f'database "{database}" does not exist' in str(e):
                print(f"Database '{database}' does not exist. Please enter a valid database name.")
            else:
                print(f"Error connecting to the database: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    # Prompt for remaining inputs
    s3_path = input("Enter S3 file path (e.g., s3://your-bucket-name/your-file.csv): ")
    iam_role = input("Enter IAM Role ARN for Redshift (e.g., arn:aws:iam::account-id:role/your-role): ")
    table_name = input("Enter the name of the Redshift table to create: ")
    schema_name = input("Enter the schema name (default: public): ") or "public"
    local_csv_path = input("Enter the local path of the CSV for schema inference: ")

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
        raise

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

        # Get temporary credentials (already obtained in get_user_inputs for database existence check)
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
