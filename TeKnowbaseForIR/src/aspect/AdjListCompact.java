package aspect;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.ImmutableValueGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import java.io.*;

import preprocess.Edge;
/**
 * class that represents a graph
 * @author pearl
 *
 */

public class AdjListCompact 
{
	
	ImmutableValueGraph<Integer, ArrayList<Integer>> labeledGraph; // graph stored as ImmutableValueGraph from Guava library
	HashMap<Integer, Integer> inverses; // maps each relationship to its inverse relationship. Used when the reverse of a given meta-path has to be found
	HashMap<String, Integer> relmap; // mapping of relationship names to their ids
	HashMap<Integer, String> relmap1; // mapping of relation id to relation string
	HashMap<Long, Set<Integer>> pairAdjList; // index on node and edge label pair to the set of nodes reachable by traversing that edge label from the node
	ImmutableValueGraph<Integer, Integer> pathGraph; // graph where the nodes are the nodes, edges exist between the two nodes if they are related by a meta-path
	ImmutableValueGraph<Integer, Integer> pathGraph_inverse; // graph representing the transpose of pathGraph
	public HashMap<Integer, ArrayList<Integer>> pathGraphHashmap;
	public HashMap<Integer, ArrayList<Integer>> pathGraphHashMapReverse;
	HashMap<Integer, Set<EndpointPair>> relIndex; // index of the edge label to node pairs that participate in a relation described by the edge label
	HashMap<Integer, String> nodeMap; // mapping of node id to node name
	HashMap<String, Integer> nodeMap1;// mapping of node name to node id
	
	public AdjListCompact(ImmutableValueGraph<Integer, ArrayList<Integer>> labeledGraph)
	{
		this.labeledGraph = labeledGraph;
	}
	
	public void setInverses(HashMap<Integer, Integer> inverses)
	{
		this.inverses = inverses;
	}
	
	public void setRelMap(HashMap<String, Integer> relmap)
	{
		this.relmap = relmap;
	}
	
	public void setPairAdjList(HashMap<Long, Set<Integer>> p)
	{
		this.pairAdjList = p;
	}
	
	/**
	 * 
	 * @param r
	 */
	
	public void returnTriplesForEdgeLabel(int r)
	{
		HashMap<Integer, Set<EndpointPair>> relIndex = new HashMap<Integer, Set<EndpointPair>>();
		for(EndpointPair p:this.labeledGraph.edges())
		{
			Integer a = (Integer) p.nodeU();
			Integer b = (Integer) p.nodeV();
			Optional c1 =  this.labeledGraph.edgeValue(a, b);
			ArrayList<Integer> c = new ArrayList<Integer>();
			if(c1.isPresent())
			{
				c = (ArrayList<Integer>) c1.get();
				//if(c.contains(115)) System.out.print("115 is present");
				//if(c==28) System.out.print("28 is present");
			}
			for(Integer cc:c)
			{
				if(r==cc)
				{
					//System.out.println("contains "+c);
					if(relIndex.get(cc)==null)
					{
						Set<EndpointPair> ss = new HashSet<EndpointPair>();
						ss.add(p);
						relIndex.put(cc,ss);
					}
					else
					{
						Set<EndpointPair> ss = relIndex.get(cc);
						ss.add(p);
						relIndex.put(cc,ss);
					}
				}
				
			}
			
		}
		this.relIndex=relIndex;
	}
	/**
	 * creates index for a pair, the source node and the edge label to the list of outgoing neighbours
	 * @return
	 */
	
	public void getListOfNeighborsForEdgeLabel(HashSet<Integer> path)
	{
		HashMap<Long, Set<Integer>> h = new HashMap<Long, Set<Integer>>();
		HashMap<Integer, Set<EndpointPair>> relIndex = new HashMap<Integer, Set<EndpointPair>>();
		for(EndpointPair p:this.labeledGraph.edges())
		{
			Integer a = (Integer) p.nodeU();
			Integer b = (Integer) p.nodeV();
			Optional c1 =  this.labeledGraph.edgeValue(a, b);
			ArrayList<Integer> c = new ArrayList<Integer>();
			if(c1.isPresent())
			{
				c = (ArrayList<Integer>) c1.get();
				//if(c.contains(115)) System.out.print("115 is present");
				//if(c==28) System.out.print("28 is present");
			}
			for(Integer cc:c)
			{
				if(path.contains(cc))
				{
					//System.out.println("contains "+c);
					if(relIndex.get(cc)==null)
					{
						Set<EndpointPair> ss = new HashSet<EndpointPair>();
						ss.add(p);
						relIndex.put(cc,ss);
					}
					else
					{
						Set<EndpointPair> ss = relIndex.get(cc);
						ss.add(p);
						relIndex.put(cc,ss);
					}
				}
				
				long l = (((long)a.intValue()) << 32) | (cc.intValue() & 0xffffffffL);
				
				if(h.get(l)==null)
				{
					Set<Integer> s = new HashSet<Integer>();
					s.add(b);
					h.put(l, s);
				}
				else
				{
					Set<Integer> s = h.get(l);
					s.add(b);
					h.put(l, s);
				}
			}
			
		}
		System.out.println(relIndex.size());
		this.pairAdjList = h;
		this.relIndex = relIndex;
		
	}
	
	/**
	 * generates random walks in a parallel fashion from each node in the graph. Parallelizes the computation of numwalks walks.
	 * @param path
	 * @param numwalks
	 * @param walklength
	 * @param outfile
	 * @throws Exception
	 */
	
	public void generateRandomWalks(ArrayList<Integer> path, int numwalks, int walklength, BufferedWriter bw, HashMap<Long, Set<Integer>> h) throws Exception
	{
		//HashMap<String, HashMap<String, Set<String>>> h = this.getListOfNeighborsForEdgeLabel();
		//BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		int num_threads = (Runtime.getRuntime().availableProcessors()); 
		int nn = numwalks;
		
		int c=0;
		for(int s:this.labeledGraph.nodes())
		{
			//System.out.println(s);
			//String s="dynamic_graph_algorithms";
			int node1 = this.randomWalk(path, 0, s, h);
			if(node1==-1) continue;
			ThreadPoolExecutor executor = new ThreadPoolExecutor(num_threads, nn, Long.MAX_VALUE, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(nn));
			HashMap<Integer, String> randomwalk = new HashMap<Integer, String>();
			for(int i=0;i<numwalks;i++)
			{
				TaskRandomWalk t1 = new TaskRandomWalk(s, path, this, walklength, h, randomwalk);
				executor.execute(t1);
			}
			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
			
			c++;
			if(c%100==0)
			{
				System.out.println(c);	
				//break;
			}
			
			bw.write(randomwalk.get(s));
			//System.out.println(randomwalk.get(s));
		}
		
		//bw.close();
		
	}
	
	/**
	 * Same as above, but parallelizes the computation of random walks over the nodes
	 * @param path
	 * @param numwalks
	 * @param walklength
	 * @param bw
	 * @param h
	 */
	
	public void generateRandomWalksAlternate(int numwalks, int walklength) throws Exception
	{
		int num_threads = (Runtime.getRuntime().availableProcessors()+1)/2; 
		int nn = pathGraph.nodes().size();
		int count=0;
		
		ThreadPoolExecutor executor = new ThreadPoolExecutor(num_threads, nn, Long.MAX_VALUE, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(nn));
		HashMap<Integer, int[][]> randomwalk = new HashMap<Integer, int[][]>();

		for(int n:this.pathGraph.nodes())
		{
			Set<Integer> s = this.pathGraph.successors(n);
			if(s.size()==0) continue;
			
			
			TaskRandomWalkAlternate t1 = new TaskRandomWalkAlternate(n,this,walklength,numwalks,randomwalk);
			executor.execute(t1);
			count++;
			
			if(count%100==0)
			{
				System.out.println(count);
			}
			
		}
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
		
	}
	
	/**
	 * 
	 * @param path: meta-path
	 * @param index: the current edge label used for retrieving the neighbors
	 * @param node: node whose neighbors for edge label indicated by 'index'
	 * @param h: index to store the set of reachable nodes from node and edge label represented by index
	 * @return
	 */
	
	public int randomWalk(ArrayList<Integer> path, int index, int node, HashMap<Long, Set<Integer>> h)
	{
		Random rand = new Random();
		if(index==path.size())
		{
			return node;
		}
		else
		{
			
				int rel = path.get(index);
				long l = (((long)node) << 32) | (rel & 0xffffffffL);
				ArrayList<Integer> candidates = new ArrayList<Integer>();
				if(h.get(l)!=null)
				{
					Set<Integer> neighbors = h.get(l);
					for(int n:neighbors)
					{
						candidates.add(n);	
					}
				}
				
				if(candidates.size()>1)
				{
					int rand1 = rand.nextInt(candidates.size()-1);
					return randomWalk(path, index+1, candidates.get(rand1), h);
				}
				else if(candidates.size()==1)
				{
					return randomWalk(path, index+1, candidates.get(0), h);
				}
				else
				{
					return -1;
				}
		}
	}
	
	/**
	 * creates the adjacency list for a meta-path.
	 * @param path: meta-path
	 * @throws Exception
	 */
	
	public void createIndexPathMasterAlternate(ArrayList<Integer> path) throws Exception
	{
		MutableValueGraph<Integer, Integer> pathGraph = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		MutableValueGraph<Integer, Integer> pathGraph_reverse = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		
		
		//HashMap<Integer,Set<EndpointPair>> relIndex = new HashMap<Integer, Set<EndpointPair>>();
		HashSet<Integer> startingNodes = new HashSet<Integer>();
		for(EndpointPair p1:relIndex.get(path.get(0)))
		{
			startingNodes.add((Integer)p1.nodeU());
		}
		for(int n:startingNodes)
		{
			HashSet<Integer> h1 = new HashSet<Integer>();
			h1.add(n);
			
			for(int p:path)
			{
				HashSet<Integer> neigh = getNeighList(h1, p);
				h1 = neigh;
			}
			if(h1.size()>0)
			{
				for(int h:h1)
				{
					pathGraph.putEdgeValue(n, h, 1);
					pathGraph_reverse.putEdgeValue(h, n, 1);
				}
			}
		}
		HashMap<Integer, ArrayList<Integer>> hh = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, ArrayList<Integer>> hh_inverse = new HashMap<Integer, ArrayList<Integer>>();
		this.pathGraph = ImmutableValueGraph.copyOf(pathGraph);
		this.pathGraph_inverse = ImmutableValueGraph.copyOf(pathGraph_reverse);
		System.out.println("number of edges in path graph = "+this.pathGraph.edges().size());
		for(EndpointPair pp:pathGraph.edges())
		{
			int u = (int) pp.nodeU();
			int v = (int) pp.nodeV();
			if(hh.get(u)==null)
			{
				ArrayList<Integer> vv = new ArrayList<Integer>();
				vv.add(v);
				hh.put(u, vv);
			}
			else
			{
				ArrayList<Integer> vv = hh.get(u);
				vv.add(v);
				hh.put(u, vv);
			}
			if(hh_inverse.get(v)==null)
			{
				ArrayList<Integer> uu = new ArrayList<Integer>();
				uu.add(u);
				hh_inverse.put(v, uu);
			}
			else
			{
				ArrayList<Integer> uu = hh_inverse.get(v);
				uu.add(u);
				hh_inverse.put(v, uu);
			}
		}
		this.pathGraphHashmap=hh;
		this.pathGraphHashMapReverse = hh_inverse;
		/*//BufferedWriter bw1 = new BufferedWriter(new FileWriter("/home/cse/phd/csz138110/scratch/dbpedia/test/pathGraph_nodes"));
		for(int n:pathGraph.nodes())
		{
			bw1.write(n+"\n");
		}
		
		BufferedWriter bw2 = new BufferedWriter(new FileWriter("/home/cse/phd/csz138110/scratch/dbpedia/test/pathGraphReverse_nodes"));
		for(int n:pathGraph_inverse.nodes())
		{
			bw2.write(n+"\n");
		}
		bw1.close();
		bw2.close();*/
		
	}
	
	/**
	 * given a path as an integer sequence, it generates an adjacency list for the same. The nodes are from the graph and there is an edge from node a to node b if there is a meta-path connecting them
	 * @param path: the meta-path
	 */
	
	public void createIndexPathMaster(ArrayList<Integer> path)
	{
		
		MutableValueGraph<Integer, Integer> pathGraph = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		MutableValueGraph<Integer, Integer> pathGraph_reverse = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		
		for(int n:this.labeledGraph.nodes())
		{
			HashSet<Integer> h1 = new HashSet<Integer>();
			h1.add(n);
			
			for(int p:path)
			{
				HashSet<Integer> neigh = getNeighList(h1, p);
				h1 = neigh;
			}
			for(int h:h1)
			{
				pathGraph.putEdgeValue(n, h, 1);
				pathGraph_reverse.putEdgeValue(h, n, 1);
			}
			
		}
		
		this.pathGraph = ImmutableValueGraph.copyOf(pathGraph);
		
		this.pathGraph_inverse = ImmutableValueGraph.copyOf(pathGraph_reverse);
		
	}
	
	/**
	 * returns the set of nodes reachable from a set h1 of nodes and edge label indicated by p
	 * @param h1: set of nodes from which the reachable nodes have to be returned
	 * @param p: edge label
	 * @return
	 */
	
	public HashSet<Integer> getNeighList(HashSet<Integer> h1, int p)
	{
		HashSet<Integer> allNeighs = new HashSet<Integer>();
		for(int node:h1)
		{
			long l = (((long)node) << 32) | (p & 0xffffffffL);
			Set<Integer> neigh = this.pairAdjList.get(l);
			if(neigh!=null)
				allNeighs.addAll(neigh);
		}
		return allNeighs;
	}
	
	/**
	 * Given a meta-path sequence indicated by a, returns another meta-path that represents its inverse. For example, "type application" becomes "application_inverse type_inverse"
	 * @param a
	 * @return
	 */
	
	public ArrayList<Integer> inverse(ArrayList<Integer> a)
	{
		ArrayList<Integer> path_inverse = new ArrayList<Integer>();
		for(int i=a.size()-1;i>=0;i--)
		{
			path_inverse.add(inverses.get(a.get(i)));
		}
		return path_inverse;
	}
	
	public void generateRandomWalksLatest(BufferedWriter bw, int walklength, int numwalks) throws Exception
	{
		int count=0;
		System.out.println(pathGraphHashmap.entrySet().size());
		//HashMap<Integer, ArrayList<ArrayList<Integer>>> randomwalk = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();

		for(int n:pathGraphHashmap.keySet())
		{
			if(pathGraphHashmap.get(n)==null) continue;
			ArrayList<Integer> s = pathGraphHashmap.get(n);
			if(s.size()==0) continue;
			count++;
			if(count%100==0)
			{
				System.out.println(count);
			}
			int num_threads = Runtime.getRuntime().availableProcessors(); 
			int nn = 1000;
			ThreadPoolExecutor executor = new ThreadPoolExecutor(num_threads,
					nn, Long.MAX_VALUE, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(nn));
						//int[][] rws = new int[1000][100];
			HashMap<Integer, String> randomwalk = new HashMap<Integer, String>();
			randomwalk.put(n, "");
			//randomwalk.put(n, aa);
			for(int num=0;num<numwalks;num++)
			{
				TaskRandomSelect t1 = new TaskRandomSelect(this, walklength, "", n, n, 1, randomwalk);
				executor.execute(t1);
			}
			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
			bw.write(randomwalk.get(n));
		}
		
	}
	
	public void findPaths(int u, int v, int k, BufferedWriter bw) throws Exception
	{
		LinkedList<Integer> queue = new LinkedList<Integer>();
		queue.add(u);
		HashMap<Integer, Integer> pred = new HashMap<Integer, Integer>();
		int count=0;
		while(queue.size()>0 && count<=k)
		{
			int a=queue.removeFirst();
			
			for(int neigh: this.labeledGraph.successors(a))
			{
				queue.add(neigh);
				
				pred.put(neigh, a);
				if(count==k && (neigh==v))
				{
					
					Optional<ArrayList<Integer>> o = this.labeledGraph.edgeValue(a, neigh);
					if(o.isPresent())
					{
						for(int j:o.get())
						{
							bw.write(this.nodeMap.get(neigh)+"\t"+this.relmap1.get(j)+"\t");
							int cc = 1;
							int a1=a;
							while(pred.get(a1)!=null && cc<k)
							{
								int eee = pred.get(a1);
								Optional<ArrayList<Integer>> o1 = this.labeledGraph.edgeValue(eee, a1);
								if(o1.isPresent())
								{
									for(int j1:o1.get())
									{
										bw.write(this.nodeMap.get(a1)+"\t"+this.relmap1.get(j1)+"\t");
									}
								}
								a1=eee;
								cc++;
							}
							
						}
					}
					
					bw.write(this.nodeMap.get(u)+"\n");
				}
			}
			count++;
			
		}
	}
	
	public void writeToFile(String path) throws Exception
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(path));
		for(EndpointPair p:this.labeledGraph.edges())
		{
			Integer a = (Integer) p.nodeU();
			Integer b = (Integer) p.nodeV();
			Optional c1 =  this.labeledGraph.edgeValue(a, b);
			ArrayList<Integer> c = new ArrayList<Integer>();
			if(c1.isPresent())
			{
				c = (ArrayList<Integer>) c1.get();
				for(Integer i:c)
				{
					bw.write(a+"\t"+i+"\t"+b+"\n");
				}
			}
		}
		bw.close();
	}
	
	
	
}
