'''
https://airflow.apache.org/docs/stable/tutorial.html
'''

from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'achen',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 25),
    'email': ['annecool37@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 10',
    retries=3,
    dag=dag,
)

t3 = BashOperator(
    task_id='echo',
    bash_command="echo 'this is task 3'",
    dag=dag,
)

tasks = ['file1', 'file2', 'file3']

def _get_bash_ops(**kwargs):
    
    fn = kwargs['fn']
    cmd = 'python write_file.py --task write_csv  --fn {}'.format(fn)
    obj = BashOperator(
            task_id='write_{}'.format(fn),
            bash_command=cmd,
            dag=kwargs['dag'],
            )

    return obj

start = DummyOperator(task_id = 'start', dag=dag)
end = DummyOperator(task_id = 'end', dag=dag)

# this is the same as
# t1 >> t2
# t1 >> t3

t1 >> [t2, t3] >> start 

fn_lst = ['file1', 'file2', 'file3']
for fn in fn_lst:
    start >> _get_bash_ops(**{'fn': fn, 'dag': dag}) >> end

task_test =  BashOperator(
    task_id='task_test',
    bash_command="echo '123'",
    # bash_command='python hello_world.py',
    dag=dag,
)

if __name__ == '__main__':
    from airflow.models import TaskInstance
    t = TaskInstance(task_test, execution_date = datetime.now())
    t.run(ignore_all_deps = True)
