# Lambda Architecture
1. Write data.csv into table hive
2. Streaming data twitter based on words [spotify com track] and send the data to kafka
3. Processing data from kafka using spark structured streaming
4. writestream into hdfs path, destionation path to path tweetspotify /user/hive/warehouse/bigproject.db/tweetspotify
5. Creating workflow for joining table hive and getting insight about top music based on tweet.
