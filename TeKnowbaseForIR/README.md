# README #


### Description of classes ###

Following are the important classes in the repository

* AdjListCompact: Class to represent a knowledge base as a graph. 

* Aspect: Class that has some utility functions to generate aspects for a query. Includes functions to generate meta-paths and perform random walks given a meta-path. The functions of interest in this class related to generation of random walks are
  
  Aspect.randomWalkMasterHPC(String kb, String outfile, String folder, String relmap): test module to check the parallelized version of randomw walks which selects each neighbor randomly
  
  Aspect.randomWalkMasterHPC1(String kb, String outfile, String folder, String relmap): test module to check code on hpc. Here the random number generation is replaced by estimating the nodes at a step given a starting node. Not complete
  
  Aspect.randomwalkAlternate(int j, int node, AdjListCompact a, int[][] rws, ImmutableValueGraph pathgraph): for an index j and j+1, instead of random walk, it assigns the array values based on the expectation of visiting the neighbours of node n
  
  Aspect.readMetaPathList(String folder1, String folder2): reads the selected meta paths listed inside folder2 and returns a list of lists
  
  Aspect.readGraphEfficientAlternate(String kb, String relMap): efficient way to read a graph. Uses Google Guava library to represent the graph compactly
  
* TaskRandomWalk: class to parallelize the random walk from a node for a given meta-path

* TaskRandomSelect: class to parallelize the random walks. A single walk of 100 steps is computed by each thread. 
