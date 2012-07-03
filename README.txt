
1. Start Redis server:
sudo /etc/init.d/redis_6379 start

2. Run the jar file as 1 master thread and 3 workers, and each worker has a pop song list of 700:
java -Xmx2048M -cp kaggle-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.zhengyang.kaggle.App master worker -t 3 -p 700