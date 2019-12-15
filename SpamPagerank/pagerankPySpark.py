
import pyspark as py
from pyspark.sql import SparkSession
import re
from operator import add



def computeContribs(urls, rank, alpha):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, alpha * rank/ num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return (parts[0], parts[1])


def pageRankComputation (ranks, links, nodes, dangling_nodes, N, tol, max_iter, alpha):
    for iteration in range(max_iter):
        last_ranks = ranks
        ranks = nodes.map(lambda url_neighbors: (url_neighbors, 0.0)) #new initializarion
        dangling_node_score = dangling_nodes.map(lambda url_neighbors: (url_neighbors, 0.0))
        dangling_node_sum = last_ranks.join(dangling_node_score).map(lambda dangling: dangling[1][0]).reduce(add)
        #Calculates URL contributions to the rank of other URLs.
        contribs = links.join(last_ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1], alpha))
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = ranks.union(contribs).reduceByKey(add).mapValues(lambda rank: rank + ((alpha * dangling_node_sum )+ 1 - alpha)/N)#
        err = ranks.union(last_ranks).reduceByKey(lambda a,b: abs(a-b)).map(lambda error: error[1]).reduce(add)
        if err < tol:
            return (ranks,  iteration)
    return (ranks, iteration)

def pageRankPySpark(spark, filename, alpha = 0.85, max_iter = 150, tol = 1.0e-4):
    lines = spark.read.text(filename).rdd.map(lambda r: r[0])
    nodes = lines.flatMap(lambda node: node.split(" ")).distinct() #all the nodes inside our collection
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache() #all the links between the nodes
    edge_node = links.map(lambda node: node[0]) #NODES THAT HAS OUTGOING LINK
    dangling_nodes = nodes.subtract(edge_node) #NODES THAT DOESN'T HAVE OUTGOING LINK 
    N = nodes.count() #TOTAL NUMBER OF NODES
    ranks = nodes.map(lambda url_neighbors: (url_neighbors, 1.0/N)) #initiazilization
    score, iteration = pageRankComputation(ranks, links, nodes, dangling_nodes, N, tol, max_iter, alpha)
    return (score, iteration)
