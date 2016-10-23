# 
# The file finishes P4.2 task1 - Count the number of edges and vertices in Twitter Social Network
# Hao Wang - haow2
# 

from pyspark import SparkConf, SparkContext

# Main Function
def main(sc):
	# Get the input file
	inputFile = sc.textFile("/mnt/Project4_2/TwitterGraph.txt")

	# Num of Edges
	num_edges = inputFile.map(lambda line: (line, 1)).distinct().count()

	# Num of vertices
	num_vertices = inputFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).distinct().count()
	
	# Print result
	print(num_edges)
	print(num_vertices)


if __name__ == "__main__":
	# Configuration
	conf = SparkConf().setAppName("Task1").set("spark.driver.memory", "100g").set("spark.executor.memory", "100g")\
			.set("spark.driver.cores", "8").set("spark.executor.cores", "40").setMaster("local[*]")
	sc = SparkContext(conf=conf)

	main(sc)