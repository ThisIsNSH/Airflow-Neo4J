from datetime import datetime, date, timedelta
import json
import os
import random
import neo4j
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

# Default arguments to be passed into tasks
default_args = {
    'owner': 'nishant',
    'email': ['hadanis.singh@gmail.com'],
}

with DAG(
        'tribesai',
        default_args = default_args,
        description = 'Assessment task for Tribes.AI',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2021, 10, 15),
        catchup = False,
        tags = ['tribesai'],
) as dag:

    # List of user email addresses
    users = [
                {
                'user_id': 'vinit@tribes.ai',
                'name': 'vinit',
                'device': {
                    'os': 'ios',
                    'brand': 'apple'
                    }
                }, {
                    'user_id': 'guilermo@tribes.ai',
                    'name': 'guilermo',
                    'device': {
                        'os': 'android',
                        'brand': 'samsung'
                    }
                }, {
                    'user_id': 'christian@tribes.ai',
                    'name': 'christian',
                    'device': {
                        'os': 'android',
                        'brand': 'mi'
                    }
                }, {
                    'user_id': 'elly@tribes.ai',
                    'name': 'elly',
                    'device': {
                        'os': 'android',
                        'brand': 'oppo'
                    }
                }, {
                    'user_id': 'nishant@tribes.ai',
                    'name': 'nishant',
                    'device': {
                        'os': 'ios',
                        'brand': 'apple'
                    }
                }
            ]

    # List of applications with 0 minutes of usage
    usages = [
                {
                    'app_name': 'slack',
                    'app_category': 'communication',
                    'minutes_used': 0
                }, {
                    'app_name': 'gmail',
                    'app_category': 'communication',
                    'minutes_used': 0
                }, {
                    'app_name': 'jira',
                    'app_category': 'task_management',
                    'minutes_used': 0
                }, {
                    'app_name': 'google drive',
                    'app_category': 'file_management',
                    'minutes_used': 0
                }, {
                    'app_name': 'chrome',
                    'app_category': 'web_browser',
                    'minutes_used': 0
                }, {
                    'app_name': 'spotify',
                    'app_category': 'entertainment_music',
                    'minutes_used': 0
                }
            ]

    ############# Neo4j Functions #############

    driver = neo4j.GraphDatabase.driver('bolt://3.236.40.179:7687',
                                        auth=neo4j.basic_auth('neo4j', 
                                        'brace-excuses-originals'))

    def create_user_node(user_id):
        return f'CREATE (n:User {{IdMaster: "{user_id}"}})'

    def create_app_node(app_name, app_category):
        return f'CREATE (n:App {{IdMaster: "{app_name}", AppCategory: "{app_category}"}})'

    def create_device_node(os):
        return f'CREATE (n:Device {{IdMaster: "{os}"}})'

    def create_brand_node(brand):
        return f'CREATE (n:Brand {{IdMaster: "{brand}"}})'

    def create_used_relationship(user_id, app_name, usage_date, minutes_used):
        return (f'MATCH (a:User), (b:App)'
                f'WHERE a.IdMaster = "{user_id}" AND b.IdMaster = "{app_name}"' 
                f'CREATE (a)-[r:USED {{TimeCreated: "{datetime.now()}", TimeEvent : "{usage_date}", ' 
                f'UsageMinutes : "{minutes_used}"}}]->(b)')

    def create_on_relationship(app_name, os):
        return (f'MATCH (a:App), (b:Device)' 
                f'WHERE a.IdMaster = "{app_name}" AND b.IdMaster = "{os}"' 
                f'CREATE (a)-[r:ON {{TimeCreated: "{datetime.now()}"}}]->(b)'	)

    def create_of_relationship(os, brand):
        return (f'MATCH (a:Device), (b:Brand)' 
                f'WHERE a.IdMaster = "{os}" AND b.IdMaster = "{brand}"' 
                f'CREATE (a)-[r:OF {{TimeCreated: "{datetime.now()}"}}]->(b)')

    def node_exists(type, id):
        return (f'OPTIONAL MATCH (n:{type} {{IdMaster: "{id}" }})'
                f'RETURN n IS NOT NULL AS Predicate')

    ############# Pipeline Functions #############

    # Check if pipeline ran for the first time
    def check_first_time_fx(last_run):
        if last_run == 'None':
            return 'generate_old_data'
        else:
            return 'generate_data'

    # Generate last 30 day's + current day's data
    def generate_old_data_fx(date):

        for i in range(31):
            generate_data_fx(date - timedelta(days=i))

    # Generate apps usage data (random)
    def generate_data_fx(date):

        # Output directory path
        dir = os.path.dirname(os.path.abspath(
            __file__)) + '/nishant_assessment/data/' + str(date)

        # Create the output directory if doesn't
        if not os.path.exists(dir):
            os.makedirs(dir)

        for user in users:
            time_left = 480

            # Generate random usage minutes
            for app in usages:
                app['minutes_used'] = min(time_left, random.randint(60, 100))
                time_left = max(time_left - app['minutes_used'], 0)

            data = {
                'user_id': user['user_id'],
                'usage_date': str(date),
                'device': user['device'],
                'usages': usages
            }

            # Save json file in the output directory
            file_name = dir + '/' + user['name'] + '_usage_data_' + str(date) + '.json'
            with open(file_name, 'w') as outfile:
                outfile.write(json.dumps(data, indent=4))

    # Populate last 30 day's + current day's data
    def populate_old_data_fx(date):

        for i in range(31):
            populate_data_fx(date - timedelta(days=i))

    # Populate apps usage data (random)
    def populate_data_fx(date):
        
        # Input directory path
        dir = os.path.dirname(os.path.abspath(
            __file__)) + '/nishant_assessment/data/' + str(date)

        for file_name in os.listdir(dir):

            with open(dir + '/' + file_name, 'r') as json_file:
                data = json.load(json_file)

                with driver.session(database='neo4j') as session:

                    # Create node if doesn't exist
                    if (not session.run(node_exists('User', data['user_id'])).single().value()):
                        session.run(create_user_node(data['user_id']))
                    
                    if (not session.run(node_exists('Device', data['device']['os'])).single().value()):
                        session.run(create_device_node(data['device']['os']))
                    
                    if (not session.run(node_exists('Brand', data['device']['brand'])).single().value()):
                        session.run(create_brand_node(data['device']['brand']))
                    
                    for usage in usages:

                        if (not session.run(node_exists('App', usage['app_name'])).single().value()):
                            session.run(create_app_node(usage['app_name'], usage['app_category']))

                        session.run(create_used_relationship(data['user_id'], usage['app_name'], data['usage_date'], usage['minutes_used']))
                        session.run(create_on_relationship(usage['app_name'], data['device']['os']))
                        session.run(create_of_relationship(data['device']['os'], data['device']['brand']))

                driver.close()

    ############# Pipeline Tasks #############

    # Check if schedule ran for the first time
    check_first_time = BranchPythonOperator(task_id = 'check_first_time',
                                            python_callable = check_first_time_fx,
                                            op_kwargs = {'last_run': '{{dag_run.get_previous_dagrun()}}'})

    # Populate last 30 days of random data for users
    generate_old_data = PythonOperator(task_id = 'generate_old_data',
                                       python_callable = generate_old_data_fx,
                                       op_kwargs = {'date': date.today()})

    # Generate random mobile usage data for users
    generate_data = PythonOperator(task_id = 'generate_data',
                                   python_callable = generate_data_fx,
                                   op_kwargs = {'date': date.today()})

    # Populate old usage data to Neo4j
    populate_old_data = PythonOperator(task_id = 'populate_old_data',
                                   python_callable = populate_old_data_fx,
                                   op_kwargs = {'date': date.today()})

    # Populate usage data to Neo4j
    populate_data = PythonOperator(task_id = 'populate_data',
                                   python_callable = populate_data_fx,
                                   op_kwargs = {'date': date.today()})

    check_first_time >> [generate_data, generate_old_data]
    generate_old_data >> populate_old_data
    generate_data >> populate_data

    dag.doc_md = '''#### Documentation
    Tribes.AI Assessment Task - In this we generate daily mobile usage data for 5 users. 
    '''
