Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    OrchestrationSource_0 = SourceTask(
        task_id = "OrchestrationSource_0", 
        component = "OrchestrationSource", 
        kind = "OnedriveSource", 
        connector = Connection(kind = "onedrive"), 
        isNew = True, 
        format = CSVFormat(allowLazyQuotes = False, allowEmptyColumnNames = True, separator = ",", nullValue = "", header = True)
    )
