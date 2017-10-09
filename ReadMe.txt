
To run the jar - Please run at the path where the jars are located.
Q.1 Syntax : hadoop jar <jar-name> <input file path> <output path>

hadoop jar MutualFriends.jar /user/sonali/soc-LiveJournal1Adj.txt /user/sonali/mutualfrnd

// To get the output folder to local
hdfs dfs -get /user/sonali/mutualfrnd

Q.2 Syntax : hadoop jar <jar-name> <input file path> <output path>

hadoop jar MutualFriendCount.jar /user/sonali/soc-LiveJournal1Adj.txt /user/sonali/mutualfrndcount

hdfs dfs -get /user/sonali/mutualfrndcount

Q.3 Syntax : hadoop jar <jar-name> <review file path> <business file path> <output path>

hadoop jar BusinessTop10.jar /user/sonali/review.csv /user/sonali/business.csv /user/sonali/top10business

hdfs dfs -get /user/sonali/top10business

Q.4 Syntax : hadoop jar <jar-name> <review file path> <output path> <business file path>

hadoop jar UserRating.jar /user/sonali/review.csv /user/sonali/userrate1 /user/sonali/business.csv

hdfs dfs -get /user/sonali/userrate1






