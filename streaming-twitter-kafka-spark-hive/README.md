# Lambda Architecture
1. Write data.csv into table hive
2. Streaming data twitter based on words [spotify com track] and send the data to kafka
3. Processing data dari kafka menggunakan spark structured streaming
4. writestream into hdfs path, path yang dituju ke path table tweetspotify /user/hive/warehouse/bigproject.db/tweetspotify
5. Creating workflow untuk join table hive for getting insight about top music based on tweet.
