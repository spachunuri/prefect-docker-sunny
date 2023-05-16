from sql_lib import run_sql_file
from prefect import flow, task

@task
def dailyrun_perfload():
    run_sql_file("dqm_daily_snapshot_perf")

@task
def dailyrun_load1():
    run_sql_file("dqm_daily_snapshot_load1")
@task
def dailyrun_load2():
    run_sql_file("dqm_daily_snapshot_load2")

@flow
def dqm_daily_snapshot():
    dailyrun_perfload()
    #dailyrun_load1()
    #dailyrun_load2()

if __name__ == "__main__":
    dqm_daily_snapshot()