with open('/opt/airflow/dags/transaction_pipeline_dag.py', 'r') as f:
    content = f.read()

# Find and replace BashOperator with PythonOperator for load_snowflake
import re
old = re.search(r'    load_snowflake = BashOperator\(.*?\)', content, re.DOTALL).group(0)
new = '''    load_snowflake = PythonOperator(
        task_id         = "load_snowflake",
        python_callable = run_load_snowflake,
    )'''

content = content.replace(old, new)

with open('/opt/airflow/dags/transaction_pipeline_dag.py', 'w') as f:
    f.write(content)

print("Done - load_snowflake is now PythonOperator")