# 
# The file finishes P4.2 task2 - Count the number of each followers in Twitter Social Network
# Hao Wang - haow2
# 

from pyspark import SparkConf, SparkContext

# Main Function
def main(sc):
	# Get the input file
	inputFile = sc.textFile("hdfs://ec2-52-91-93-135.compute-1.amazonaws.com/input/TwitterGraph.txt")

	# Get all followers
	followers = inputFile.distinct().map(lambda line: (line.split("\t")[1], 1)).reduceByKey(lambda a, b: a + b)

	# Change the output format
	# Referenced from http://stackoverflow.com/questions/31898964/how-to-write-the-resulting-rdd-to-a-csv-file-in-spark-python
	output = followers.map(changeFormat)

	# Save
	output.saveAsTextFile("hdfs://ec2-52-91-93-135.compute-1.amazonaws.com/follower-output")


# Change the format of data
def changeFormat(data):
	return '\t'.join(str(d) for d in data)


if __name__ == "__main__":
	# Configuration
	conf = SparkConf().setAppName("Task1").set("spark.driver.memory", "100g").set("spark.executor.memory", "100g").setMaster("local[*]")
	sc = SparkContext(conf=conf)

	main(sc)
