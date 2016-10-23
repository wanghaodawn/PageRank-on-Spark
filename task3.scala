/*
 * This file finishes P4.2 task3 - Implemented PageRank in Twitter Social Network
 * Hao Wang - haow2
 */ 

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object PageRank {
	def main (args: Array[String]) {
		// Configuration
		val conf = new SparkConf().setAppName("task3_pagerank")
		val sc = new SparkContext(conf)

		// Get the links from the given data
		val links = sc.textFile("s3://cmucc-datasets/TwitterGraph.txt", 32).distinct().cache()

		// Define the number of iterations
		val num_iter = 10

		// Define the number of users
		val num_user = 2315848.0

		// Get all followers and followees
		var followers = links.map(line => line.split("\t")(0)).cache()
		var followees = links.map(line => line.split("\t")(1)).cache()

		// Get dangling users and set initial ranks as 1.0
		var dangling_users = followees.subtract(followers).map(rank => (rank, 1.0)).cache()

		// Set initial ranks of all nodes as 1.0
		var ranks = followees.map(rank => (rank, 1.0)).cache()

		// Cache all list of links grouped by key
		var list = links.map(line => (line.split("\t")(0), line.split("\t")(1))).groupByKey().cache()

		// Iterations
		for (i <- 1 to num_iter) {
			println("\nIteration: " + i + " Begin!\n")

			// Combine neighour and rank then calculate contributions
			var contribs = list.join(ranks).flatMap{ 
				case (id, (neighour, rank)) =>
        		neighour.map(id => (id, rank/neighour.size))
      		}.reduceByKey(_ + _)

      		// Get the ranks of dangling nodes
      		var dangling_ranks = dangling_users.join(ranks).map{
        		case (id, (neighour, rank)) => rank
      		}.reduce(_ + _)

      		// Update the value of ranks
      		ranks = contribs.mapValues(rank => 0.15 + 0.85 * (rank + dangling_ranks/num_user))

			println("\nIteration: " + i + " Finished!\n")
		}

		// Change the format of data and save results to file
		var res = ranks.map(line => line._1 + "\t" + line._2)
		res.saveAsTextFile("hdfs:///pagerank-output")
	}
}
