
import pyspark as py
from pyspark.sql import SparkSession
import re
from operator import add



def computeContribs(urls, rank, alpha):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, alpha * rank/ num_urls)


def parseIncoming(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return (parts[1], parts[0])        
        
def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return (parts[0], parts[1])

def numIncomingLink (lines, nodesIncOutLink):
    print('Computing the number of Incoming Links')
    incomingLink = lines.map(lambda urls: parseIncoming(urls)).distinct().groupByKey().cache().map(lambda x : (x[0], len(x[1]))).union(nodesIncOutLink).reduceByKey(add)
    return (incomingLink)

def numOutgoingLink (links, nodesIncOutLink):
    print('Computing the number of Outgming Links')
    outgoingLink =links.map(lambda node : (node[0], len(node[1])), preservesPartitioning = True).union(nodesIncOutLink).reduceByKey(add)
    return (outgoingLink)

def incomingOutgoingLinkComputation (lines, links, nodes):
    nodesIncOutLink = nodes.map(lambda x: (x, 0))
    incomingLink = numIncomingLink (lines, nodesIncOutLink)
    outgoingLink = numOutgoingLink (links, nodesIncOutLink)
    return(incomingLink, outgoingLink)


# def pageRankComputation (ranks, links, nodes, dangling_nodes, N, tol, max_iter, alpha):
#     dangling_node_score = dangling_nodes.map(lambda url_neighbors: (url_neighbors, 0.0))
#     for iteration in range(max_iter):
#         print(iteration)
#         last_ranks = ranks
#         ranks = nodes.map(lambda url_neighbors: (url_neighbors, 0.0)) #new initializarion
#         dangling_node_sum = last_ranks.join(dangling_node_score).map(lambda dangling: dangling[1][0]).reduce(add)
#         print('computed dang sum')
#         #Calculates URL contributions to the rank of other URLs.
#         contribs = links.join(last_ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1], alpha))
#         # Re-calculates URL ranks based on neighbor contributions.
#         ranks = ranks.union(contribs).reduceByKey(add).mapValues(lambda rank: rank + ((alpha * dangling_node_sum )+ 1 - alpha)/N)#
#         print('error going to be computed')

#         err = ranks.union(last_ranks).reduceByKey(lambda a,b: abs(a-b)).map(lambda error: error[1]).reduce(add)
#         if err < tol:
#             return (ranks,  iteration)
#     return (ranks, iteration)



# def pageRankPySpark(spark, filename, alpha = 0.85, max_iter = 150, tol = 1.0e-4, number_of_nodes = -1):
#     lines = spark.read.text(filename).rdd.map(lambda r: r[0])
#     nodes = lines.flatMap(lambda node: node.split(" ")).distinct() #all the nodes inside our collection
#     links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache() #all the links between the nodes
#     if number_of_nodes == -1:
#         nodes = lines.flatMap(lambda node: node.split(" ")).distinct() #all the nodes inside our collection
#         N = nodes.count() #TOTAL NUMBER OF NODES
#     else:
#         N = number_of_nodes 
#         nodes = spark.range(0, N).rdd.flatMap(list).map(str) #all the nodes inside our collection

#     edge_node = links.map(lambda node: node[0]) #NODES THAT HAS OUTGOING LINK
#     dangling_nodes = nodes.subtract(edge_node) #NODES THAT DOESN'T HAVE OUTGOING LINK 
#     N = nodes.count() #TOTAL NUMBER OF NODES
#     ranks = nodes.map(lambda url_neighbors: (url_neighbors, 1.0/N)) #initiazilization
#     score, iteration = pageRankComputation(ranks, links, nodes, dangling_nodes, N, tol, max_iter, alpha)
#     return (score, iteration)




def pageRankComputationOpt (ranks, links, nodes, dangling_nodes, N, tol, max_iter, alpha):
    for iteration in range(max_iter):
        print('iteration', iteration)
        last_ranks = ranks
        ranks = nodes.map(lambda url_neighbors: (url_neighbors, 0.0), preservesPartitioning = True) #new initializarion
        print('computing the dangling_node_score...')
        dangling_node_score = dangling_nodes.map(lambda url_neighbors: (url_neighbors, 0.0), preservesPartitioning = True)
        if iteration == 0:
            dangling_node_sum = dangling_nodes.count()* (1/N)            
        else:
             dangling_node_sum = last_ranks.join(dangling_node_score, numPartitions = links.getNumPartitions()).map(lambda dangling: dangling[1][0]).cache().reduce(add)

        #Calculates URL contributions to the rank of other URLs.
        contribs = links.join(last_ranks, numPartitions = links.getNumPartitions()).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1], alpha))
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = ranks.union(contribs).reduceByKey(add, numPartitions = links.getNumPartitions()).mapValues(lambda rank: rank + ((alpha * dangling_node_sum )+ 1 - alpha)/N)#
        print('computing the error...')
        err = ranks.union(last_ranks).reduceByKey(lambda a,b: abs(a-b), numPartitions = links.getNumPartitions()).map(lambda error: error[1]).reduce(add)
        if err < tol:
             return (ranks,  iteration)
    return (ranks, iteration)



    
    
def networkDataComputation(spark, filename, nodesFile, alpha = 0.85, max_iter = 150, tol = 1.0e-4, number_of_nodes = -1, compute_pagerank = True):
    print('Starting Network Analysis: after this computation we will have for each nodes the Number of Outgoing and Incoming Link and the pagerank score')
    #reading the file -> ['goingTo1 pointingTo1', 'goingTo2 pointingTo2', ....]
    lines = spark.read.text(filename).rdd.map(lambda r: r[0])
   
    nodes = spark.read.text(nodesFile).rdd.map(lambda r: r[0])    
    if number_of_nodes == -1:
#         nodes = lines.flatMap(lambda node: node.split(" ") , preservesPartitioning = True).distinct() #all the nodes inside our collection
        N = nodes.count() #TOTAL NUMBER OF NODES
    else:
        N = number_of_nodes 
#         nodes = spark.range(0, N).rdd.flatMap(list).map(str) #all the nodes inside our collection

    #all the links between the nodes
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().partitionBy(300).cache() 
    
    if compute_pagerank:
        #for each nodes compute the numbers of outgoing and incoming links
        incomingLink, outgoingLink = incomingOutgoingLinkComputation (lines, links, nodes)


        edge_node = links.map(lambda node: node[0], preservesPartitioning = True) #NODES THAT HAS OUTGOING LINK
        dangling_nodes = nodes.subtract(edge_node, numPartitions = links.getNumPartitions()) #NODES THAT DOESN'T HAVE OUTGOING LINK 
        ranks = nodes.map(lambda url_neighbors: (url_neighbors, 1.0/N), preservesPartitioning = True) #initiazilization

        print('Starting with the pagerank Computations. Expected 42 iteration')
        score, iteration = pageRankComputationOpt(ranks, links, nodes, dangling_nodes, N, tol, max_iter, alpha)
        return(outgoingLink, incomingLink, score, iteration)
    else:
        incomingLink, outgoingLink = incomingOutgoingLinkComputation (lines, links, nodes)
        return(incomingLink, outgoingLink)




