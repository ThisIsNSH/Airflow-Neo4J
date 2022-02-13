# Script 

## Steps to Run 

* Paste the *tribesai.py* file to /airflow/dags/ folder
* Start the airflow scheduler using the command *airflow schedule*
* Start the airflow webserver to view the progress using the command *airflow webserver*
* Unpause the dag using airflow webserver of via command *airflow dags unpause tribesai*

## High-Level Code Breakdown 

The code is divided into multiple sections separated by #

* Starting Section - Contains basic initializations required
* Neo4j Functions - Contains functions related to Neo4j eg. to create nodes, relationships and check if nodes exists
* Pipeline Functions - Contains functions which are called via pipeline tasks 
* Pipeline Tasks - Contains tasks used in pipeline and their ordering

## Low-Level Code Breakdown

### Starting Section
* We begin with declaring default arguments for the DAG
* Next, we create the DAG object
* Later, we declare the users and the apps they will/might use 

### Neo4j Functions
* create_user_node - This function helps in creating User nodes in Neo4j
* create_app_node - This function helps in creating App nodes in Neo4j
* create_device_node - This function helps in creating Device nodes in Neo4j
* create_brand_node - This function helps in creating Brand nodes in Neo4j
* create_used_relationship - This function helps in creating used relationship between User and App in Neo4j
* create_on_relationship - This function helps in creating on relationship between App and Device in Neo4j
* create_of_relationship - This function helps in creating of relationship between Device and Brand in Neo4j
* node_exists - This function helps in checking if node of give type and idmaster property exists in Neo4j

### Pipeline Functions
* check_first_time_fx - This function helps in checking if the pipeline is run for the first time. As an argument it takes in the value of *dag_run.get_previous_dagrun()* which is a macro defined in airflow. It returns the task_id based on the condition
* generate_old_data_fx - This function helps in generating last 30 day's and current day's of data using the generate_data_fx function. 
* generate_data_fx - This function helps in generating data for the users for a specific date and save it in json format 
* populate_old_data_fx - This function reads the data file, parses it and populates the Neo4j database for current date and last 30 days
* populate_data_fx - This function reads the data file, parses it and populates the Neo4j database for current date

### Pipeline Tasks
* check_first_time - This is a BranchPythonOperator tasks which checks the condition in check_first_time_fx and runs the next task accordingly 
* generate_old_data - This is a PythonOperator task that calls the generate_old_data_fx function 
* generate_data - This is a PythonOperator task that calls the generate_data_fx function 
* populate_old_data - This is a PythonOperator task that calls the populate_old_data_fx function 
* populate_data - This is a PythonOperator task that calls the populate_data_fx function 