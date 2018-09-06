package aspect;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.io.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import preprocess.AdjList;
import preprocess.Edge;
import preprocess.ReadSubgraph;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraphBuilder;

public class AdjListCompactOld {

	/**
	 * @param args
	 * 
	 */
	MutableValueGraph<String, String> labeledGraph;
	
	
	public AdjListCompactOld(MutableValueGraph<String, String> labeledGraph)
	{
		this.labeledGraph = labeledGraph;
	}
	
	/**
	 * generates random walks in a parallel fashion from each node in the graph. Parallelizes the computation of numwalks walks.
	 * @param path
	 * @param numwalks
	 * @param walklength
	 * @param outfile
	 * @throws Exception
	 */
	
	public void generateRandomWalks(ArrayList<String> path, int numwalks, int walklength, BufferedWriter bw, HashMap<String, HashMap<String, Set<String>>> h) throws Exception
	{
		//HashMap<String, HashMap<String, Set<String>>> h = this.getListOfNeighborsForEdgeLabel();
		//BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		int num_threads = (Runtime.getRuntime().availableProcessors()); 
		int nn = numwalks;
		
		int c=0;
		for(String s:this.labeledGraph.nodes())
		{
			//System.out.println(s);
			//String s="dynamic_graph_algorithms";
			String node1 = this.randomWalk(path, 0, s, h);
			if(node1.equals("")) continue;
			ThreadPoolExecutor executor = new ThreadPoolExecutor(num_threads,
					nn, Long.MAX_VALUE, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(nn));
			HashMap<String, String> randomwalk = new HashMap<String, String>();
			for(int i=0;i<numwalks;i++)
			{
				TaskRandomWalkOld t1 = new TaskRandomWalkOld(s, path, this, walklength, h,randomwalk);
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
	
	
	
	
	
	public ArrayList<String> inverse(ArrayList<String> a)
	{
		ArrayList<String> path_inverse = new ArrayList<String>();
		for(int i=a.size()-1;i>=0;i--)
		{
			if(a.get(i).endsWith("_inverse"))
			{
				path_inverse.add(a.get(i).replace("_inverse", ""));
			}
			else
			{
				path_inverse.add(a.get(i)+"_inverse");
			}
		}
		return path_inverse;
	}
	
	public String randomWalk(ArrayList<String> path, int index, String node, HashMap<String, HashMap<String, Set<String>>> h)
	{
		Random rand = new Random();
		if(index==path.size())
		{
			return node;
		}
		else
		{
			String rel = path.get(index);
			ArrayList<String> candidates = new ArrayList<String>();
			if(h.get(node)!=null)
			{
				if(h.get(node).get(rel)!=null)
				{
					Set<String> neighbors = h.get(node).get(rel);
					for(String n:neighbors)
					{
						 candidates.add(n);	
					}
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
				return "";
			}
		}
	}
	
	public MutableValueGraph<String, String> pathNeighboursMaster(HashMap<String, Set<EndpointPair>> h, ArrayList<String> path)
	{
		MutableValueGraph<String, String> mv = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		
		 
		pathNeighbours(mv, path, 0, h);
		//System.out.println(mv.edges().size());
		return mv;
	}
	
	public void pathNeighbours(MutableValueGraph<String, String> mv, ArrayList<String> path, int index, HashMap<String, Set<EndpointPair>> h)
	{
		if(index==path.size())
		{
			return;
		}
		else
		{
			String s = path.get(index);
			Set<EndpointPair> s1 = h.get(s);
			if(index==0)
			{
				for(EndpointPair e:s1)
				{
					mv.putEdgeValue((String) e.nodeU(), (String) e.nodeV(), "");
				}
			}
			else
			{
				for(EndpointPair e:s1)
				{
					String a = (String) e.nodeU();
					Set<String> set = mv.predecessors(a);
					for(String ss:set)
					{
						Set<String> succ = mv.successors(ss);
						for(String ss1:succ)
						{
							mv.removeEdge(ss, ss1);
						}
					}
					for(String ss:set)
					{
						mv.putEdgeValue(ss, (String) e.nodeV(),"");
					}
				}
			}
			pathNeighbours(mv, path, index+1, h);
		}
	}
	
	public HashMap<String, Set<EndpointPair>> generateMapOfLabels()
	{
		HashMap<String, Set<EndpointPair>> h = new HashMap<String, Set<EndpointPair>>();
		
		Set<EndpointPair<String>> s = this.labeledGraph.edges();
		for(EndpointPair p:s)
		{
			String a = (String) p.nodeU();
			String b = (String) p.nodeV();
			Optional c1 =  this.labeledGraph.edgeValue(a, b);
			String c="";
			if(c1.isPresent())
			{
				c = (String) c1.get();
			}
			Set<EndpointPair> s1; 
			
			if(h.get(c)==null)
			{
				s1 = new HashSet<EndpointPair>();
			}
			else
			{
				s1 = h.get(c);
			}
			s1.add(p);
			h.put(c, s1);
		}
		System.out.println(h.size());
		return h;
	}
	
	/**
	 * creates index on a pair (source vertex, edge label) to the list of neighbours reachable on that index
	 * @return
	 */
	
	public HashMap<String, HashMap<String, Set<String>>> getListOfNeighborsForEdgeLabel()
	{
		HashMap<String, HashMap<String, Set<String>>> h = new HashMap<String, HashMap<String, Set<String>>>();
		for(EndpointPair p:this.labeledGraph.edges())
		{
			String a = (String) p.nodeU();
			String b = (String) p.nodeV();
			Optional c1 =  this.labeledGraph.edgeValue(a, b);
			String c="";
			if(c1.isPresent())
			{
				c = (String) c1.get();
			}
			HashMap<String, Set<String>> h1;
			if(h.get(a)==null)
			{
				h1 = new HashMap<String, Set<String>>();
				HashSet<String> ss = new HashSet<String>();
				ss.add(b);
				h1.put(c, ss);
			}
			else
			{
				h1 = h.get(a);
				Set<String> ss = h1.get(c);
				if(ss==null)
				{
					ss = new HashSet<String>();
				}
				ss.add(b);
				h1.put(c, ss);
			}
			h.put(a, h1);
			
		}
		return h;
	}
	
	
	/**
	 * given a graph, this function extracts all meta-paths of a given type, unusable
	 * @param relation
	 * @param outfile
	 * @param kb1
	 * @param kb2
	 * @param length
	 * @throws Exception
	 */
	
	public static void generateMetaPathsOfLengthMaster(String relation, String outfile, String kb1, String kb2, int length) throws Exception
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		ReadSubgraph rr = new ReadSubgraph();
		AdjList a = rr.readKB(new BufferedReader(new FileReader(kb1)));
		AdjList app = a.returnTriplesOfName(relation);
		AdjList aa = rr.readKB(new BufferedReader(new FileReader(kb2)));
		AdjList a1 = aa.makeCopy();
		System.out.println("number of edges before "+a1.numberOfEdges());
		for(String s:a.getAdjList().keySet())
		{
			ArrayList<Edge> e = a.getAdjList().get(s);
			for(Edge ee:e)
			{
				if(ee.getRelationaName().equals("owl:sameas") || ee.getRelationaName().equals("owl#sameas")) continue;
				Edge ee1 = new Edge(0,s,ee.getRelationaName()+"_inverse");
				a1.getAdjList().get(ee.getName()).add(ee1);
			}
		}
		
		/*BufferedWriter bw2 = new BufferedWriter(new FileWriter("/home/prajna/tkb_with_inverses"));
		a1.writeToFile(bw2);
		bw2.close();
		*/
		System.out.println("number of edges after "+a1.numberOfEdges());
		ArrayList<Edge> eee = new ArrayList<Edge>();
		System.out.println("number of edges in: "+app.numberOfEdges());
		for(String aa1:app.getAdjList().keySet())
		{
			if(a1.getAdjList().get(aa1)==null) continue;
			ArrayList<Edge> e1 = app.getAdjList().get(aa1);
			for(Edge e:e1)
			{
				if(a1.getAdjList().get(e.getName())==null) continue;
				a1.allKHopPaths(aa1, e.getName(),length, bw);
			}
		}
		bw.close();
		BufferedReader br = new BufferedReader(new FileReader(outfile));
		BufferedWriter bw1 = new BufferedWriter(new FileWriter(outfile.replace(".tsv", "")+"_with_counts.tsv"));
		String line;
		HashMap<String, Integer> meta_paths = new HashMap<String, Integer>();
		
		while((line=br.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> list1 = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				list1.add(tok.nextToken());
			}
			String l = "";
			for(int k=list1.size()-2;k>=1;k=k-2)
			{
				l = l + list1.get(k)+"\t";
			}
			if(meta_paths.get(l)!=null)
			{
				meta_paths.put(l, meta_paths.get(l)+1);
			}
			else
			{
				meta_paths.put(l, 1);
			}
		}
		ArrayList<Entry<String, Integer>> aa1 = new ArrayList<Entry<String, Integer>>(meta_paths.entrySet());
		//Collections.sort(aa1,valueComparator);
		for(Entry<String, Integer> dd:aa1)
		{
			bw1.write(dd.getKey()+dd.getValue()+"\n");
		}
		bw1.close();
	}
	/**
	 * 
	 * @param kb
	 * @param outfile
	 * @throws Exception
	 */
	
	public void addTriplesAnotherKB(String kb, String outfile) throws Exception
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		Aspect a1 = new Aspect();
		AdjListCompactOld a = a1.readGraphEfficient(kb);
		System.out.println("read dbpedia");
		Set<String> nodes = new HashSet<String>(this.labeledGraph.nodes());
		
		for(EndpointPair p:a.labeledGraph.edges())
		{
			String u = (String) p.nodeU();
			String v = (String) p.nodeV();
			Optional c1 =  a.labeledGraph.edgeValue(u, v);
			String c="";
			if(c1.isPresent())
			{
				c = (String) c1.get();
				if(nodes.contains(u) && nodes.contains(v))
				{
					bw.write(u+"\t"+c+"\t"+v+"\n");
				}
			}
			
		}
		/*for(EndpointPair p:this.labeledGraph.edges())
		{
			String u = (String) p.nodeU();
			String v = (String) p.nodeV();
			Optional c1 =  this.labeledGraph.edgeValue(u, v);
			String c="";
			if(c1.isPresent())
			{
				c = (String) c1.get();
				bw.write(u+"\t"+c+"\t"+v+"\n");
			}
			
		}*/
		bw.close();
	}
	
	
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub

	}

}
