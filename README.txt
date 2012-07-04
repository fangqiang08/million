
1. Start Redis server:
sudo /etc/init.d/redis_6379 start

2. Run the jar file as 1 master thread and 3 workers, and each worker has a pop song list of 700:
java -Xmx2048M -cp kaggle-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.zhengyang.kaggle.App master worker -t 3 -p 700

3. Load colistened matrix into Redis
java -Xmx2048M -cp kaggle-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.zhengyang.kaggle.utils.RedisHelper



Queues in Redis:
TASK_RECORD_Q       = "TASK_RECORD_Q";
REMAINING_TASK_Q    = "REMAINING_TASK_Q";
WORKING_Q           = "WORKING_Q";
RESULT_KEY_Q        = "RESULT_KEY_Q";
MESSAGE_Q           = "MESSAGE_Q";
COMMAND_Q           = "COMMAND_Q";
RESULT_HASH         = "RESULT_HASH";
COLISTENED_HASH     = "COLISTENED_HASH";