Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    p22__Aggregate_0 = Task(task_id = "p22__Aggregate_0", component = "Model", modelName = "p22__Aggregate_0")
