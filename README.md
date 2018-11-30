# glasgow-stream-processing

## How to use
- Package using Intellij Idea artifact builder or ```sbt assembly``` plugin
- ```dictinary.txt``` and ```preprocessed_train.csv``` should be in hdfs
- ```/spark/bin/spark-submit --master yarn --class Classifier stream_processing.jar``` to train and save model in hdfs
- ```/spark/bin/spark-submit --master yarn --class StreamProcessing stream_processing.jar '<link to rss feed>'``` to run stream processor
- result of stream processing will be printed in terminal and in file ```processing_result.txt``` in hdfs
