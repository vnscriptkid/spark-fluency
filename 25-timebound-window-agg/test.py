import time

class TestGold:
    def run(self):
        # Drop existing tables
        spark.sql("drop table if exists trade_kafka")
        spark.sql("drop table if exists trade_summary")

        # Delete existing checkpoint directory
        checkpoint_dir = "/tmp/checkpoints/trade_summary"
        dbutils.fs.rm(checkpoint_dir, True)

        # # Recreate the trade_kafka table
        spark.sql("create table if not exists trade_kafka (key string, value string) using delta")

        gold = Gold()
        print('Init')
        goldQuery = gold.run()
        print('Ran gold processing')

        data = [
            ("1", '{"CreatedTime":"2023-05-23 12:01:19", "Type":"BUY", "Amount":100.0, "BrokerCode":"XYZ"}'),
            ("2", '{"CreatedTime":"2023-05-23 12:03:19", "Type":"SELL", "Amount":200.0, "BrokerCode":"ABC"}'),
            ("3", '{"CreatedTime":"2023-05-23 12:05:00", "Type":"BUY", "Amount":150.0, "BrokerCode":"XYZ"}')
        ]

        for key, value in data:
            sqlQuery = f"INSERT INTO trade_kafka VALUES ('{key}', '{value}')"
            print(sqlQuery)
            spark.sql(sqlQuery)

        # Wait for the streaming query to initialize
        print('Sleep for 30 secs')
        time.sleep(30)

        result = spark.sql(f"SELECT * FROM trade_summary")
        result.show()
        result_collect = result.collect()
        actualStart = result_collect[0][0]
        actualEnd = result_collect[0][1]
        actualTotalBuy = result_collect[0][2]
        actualTotalSell = result_collect[0][3]
        print(actualStart)
        print(actualEnd)
        print(actualTotalBuy)
        print(actualTotalSell)
        assert actualTotalBuy == 250.0
        assert actualTotalSell == 200.0

        print('Stopping')
        goldQuery.stop()

# Execute the integration test
print("Running integration test")
test = TestGold()
test.run()
print('Done')