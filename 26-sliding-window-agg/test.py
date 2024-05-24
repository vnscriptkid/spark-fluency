import time
from datetime import datetime 

class TestGold:
    def run(self):
        # Drop existing tables
        spark.sql("drop table if exists sensor_kafka")
        spark.sql("drop table if exists sensor_summary")

        # Delete existing checkpoint directory
        checkpoint_dir = "/tmp/checkpoints/sensor_summary"
        dbutils.fs.rm(checkpoint_dir, True)

        # Recreate the sensor_kafka table
        spark.sql("create table if not exists sensor_kafka (key string, value string) using delta")

        gold = Gold()
        print('Init')
        goldQuery = gold.run()
        print('Ran gold processing')

        # window size: 15 minutes, slide size: 5 minutes
        data = [
            ("1", '{"CreatedTime":"2023-05-23 12:00:00", "Reading":100.0}'),
            ("2", '{"CreatedTime":"2023-05-23 12:03:19", "Reading":200.0}'),
            ("3", '{"CreatedTime":"2023-05-23 12:05:00", "Reading":150.0}'),
            ("4", '{"CreatedTime":"2023-05-23 12:06:00", "Reading":50.0}'),
            ("5", '{"CreatedTime":"2023-05-23 12:07:00", "Reading":250.0}'),
            ("6", '{"CreatedTime":"2023-05-23 12:08:00", "Reading":300.0}'),
            ("7", '{"CreatedTime":"2023-05-23 12:09:00", "Reading":350.0}'),
            ("8", '{"CreatedTime":"2023-05-23 12:10:00", "Reading":400.0}'),
            ("9", '{"CreatedTime":"2023-05-23 12:11:00", "Reading":450.0}'),
            ("10", '{"CreatedTime":"2023-05-23 12:12:00", "Reading":500.0}'),
            ("11", '{"CreatedTime":"2023-05-23 12:13:00", "Reading":550.0}'),
            ("12", '{"CreatedTime":"2023-05-23 12:14:00", "Reading":600.0}'),
            ("13", '{"CreatedTime":"2023-05-23 12:15:00", "Reading":650.0}'),
            ("14", '{"CreatedTime":"2023-05-23 12:16:00", "Reading":700.0}'),
            ("15", '{"CreatedTime":"2023-05-23 12:17:00", "Reading":750.0}'),
            ("16", '{"CreatedTime":"2023-05-23 12:18:00", "Reading":800.0}'),
            ("17", '{"CreatedTime":"2023-05-23 12:19:00", "Reading":850.0}'),
            ("18", '{"CreatedTime":"2023-05-23 12:20:00", "Reading":900.0}'),
            ("19", '{"CreatedTime":"2023-05-23 12:21:00", "Reading":950.0}'),
            ("20", '{"CreatedTime":"2023-05-23 12:22:00", "Reading":1000.0}')
        ]

        records = []
        for key, value in data:
            record = f"('{key}', '{value}')"
            records.append(record)

        records_str = ', '.join(records)
        sqlQuery = f"INSERT INTO sensor_kafka VALUES {records_str}"
        print(sqlQuery)
        spark.sql(sqlQuery)

        # Wait for the streaming query to initialize
        print('Sleep for 30 secs')
        time.sleep(30)

        result = spark.sql(f"SELECT * FROM sensor_summary ORDER BY start")
        result_collect = result.collect()

        expected_df = spark.createDataFrame([
            (datetime(2023, 5, 23, 11, 50), datetime(2023, 5, 23, 12, 5), 200.0),
            (datetime(2023, 5, 23, 11, 55), datetime(2023, 5, 23, 12, 10), 350.0),
            (datetime(2023, 5, 23, 12, 0), datetime(2023, 5, 23, 12, 15), 600.0),
            (datetime(2023, 5, 23, 12, 5), datetime(2023, 5, 23, 12, 20), 850.0),
            (datetime(2023, 5, 23, 12, 10), datetime(2023, 5, 23, 12, 25), 1000.0),
            (datetime(2023, 5, 23, 12, 15), datetime(2023, 5, 23, 12, 30), 1000.0),
            (datetime(2023, 5, 23, 12, 20), datetime(2023, 5, 23, 12, 35), 1000.0)
        ], ["start", "end", "MaxReading"])
        expected_df_collect = expected_df.collect()

        # Sort the collected results to ensure order is same for comparison
        result_collect_sorted = sorted(result_collect, key=lambda row: row["start"])
        expected_df_collect_sorted = sorted(expected_df_collect, key=lambda row: row["start"])

        # assert result_collect_sorted == expected_df_collect_sorted
        for actual, expected in zip(result_collect_sorted, expected_df_collect_sorted):
            print(f"Actual: {actual} | Expected: {expected}")
            assert actual == expected, f"Mismatch: Actual: {actual} | Expected: {expected}"


        print('Stopping')
        goldQuery.stop()

# Execute the integration test
print("Running integration test")
test = TestGold()
test.run()
print('Done')