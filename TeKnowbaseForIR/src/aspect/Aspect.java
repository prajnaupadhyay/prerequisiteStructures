package aspect;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.io.*;
/** import statements for fastutil library
import it.unimi.dsi.fastutil.doubles.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.chars.*;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.fastutil.*;*/

import com.google.common.graph.*;


import buildAndProcessTrees.GetPropertyValues;

import preprocess.AdjList;
import preprocess.Edge;
import preprocess.Preprocess;
import preprocess.ReadSubgraph;

import LMForTeKnowbase.LanguageModelEntity;

public class Aspect 
{
	HashMap<String, ArrayList<String>> aspectNameToEntities;
	
	HashMap<String, HashMap<String, LanguageModelEntity>> aspectNameToEmbeddingSpace;
	
	static Comparator<Entry<String, Integer>> valueComparator = new Comparator<Entry<String, Integer>>() 
			{
				 public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2)
				 {
					 Integer v1 = e1.getValue();
					 Integer v2 = e2.getValue();
					 return v2.compareTo(v1);
				 }
			};
			static Comparator<Entry<String, Double>> doubleComparator = new Comparator<Entry<String, Double>>() 
					{
						 public int compare(Entry<String, Double> e1, Entry<String, Double> e2)
						 {
							 Double v1 = e1.getValue();
							 Double v2 = e2.getValue();
							 return v2.compareTo(v1);
						 }
					};
	
	public Aspect()
	{
		
	}
					
	public Aspect(String algoEmbedding, String applicationEmbedding, String techniqueEmbedding, String impEmbedding, String entityMap) throws Exception
	{
		HashMap<String, ArrayList<String>> h1 = new HashMap<String, ArrayList<String>>();
		HashMap<String, HashMap<String, LanguageModelEntity>> a1 = new HashMap<String, HashMap<String, LanguageModelEntity>>();
		this.aspectNameToEntities = h1;
		HashMap<String, String> stringToId = LanguageModelEntity.createStringToIdMapping(entityMap);
		HashMap<String, LanguageModelEntity> emb1 = LanguageModelEntity.readEmbeddingsFromFile(algoEmbedding,stringToId);
		HashMap<String, LanguageModelEntity> emb2 = LanguageModelEntity.readEmbeddingsFromFile(applicationEmbedding,stringToId);
		HashMap<String, LanguageModelEntity> emb3 = LanguageModelEntity.readEmbeddingsFromFile(techniqueEmbedding,stringToId);
		HashMap<String, LanguageModelEntity> emb4 = LanguageModelEntity.readEmbeddingsFromFile(impEmbedding,stringToId);
		a1.put("algorithm", emb1);
		a1.put("application", emb2);
		a1.put("technique", emb3);
		a1.put("impl", emb4);
		this.aspectNameToEmbeddingSpace = a1;
		
	}
	
	/**
	 * function that creates a graph of prerequisites
	 * @param input
	 * @param table
	 * @param entityTable
	 * @param graph
	 * @param outfile
	 * @param dir
	 * @param threshold
	 * @throws Exception
	 */
	
	public void generateAspects(String input, String table, String entityTable, String graph, String outfile, String dir, double threshold, String entityMap, String embedding1, String embedding2, String embedding3, String embedding4) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line;
		ArrayList<String> inputConcepts = new ArrayList<String>();
		while((line=br.readLine())!=null)
		{
			inputConcepts.add(line);
		}
		ReadSubgraph rr = new ReadSubgraph();
		AdjList aa = rr.readFromFile(new BufferedReader(new FileReader(graph)));
		aa.computeRefDValuesForAllConceptsDatabase(inputConcepts, table, entityTable, aa);
		aa.generatePrerequisiteGraphOriginal(input, table, entityTable, 0.02, dir);
		HashMap<String, ArrayList<Edge>> prerequisiteGraph = new HashMap<String, ArrayList<Edge>>();
		for(String i:inputConcepts)
		{
			BufferedReader br1 = new BufferedReader(new FileReader("/mnt/dell/prajna/refdScores/"+dir+"/"+i+"/prerequisiteGraphOriginal"+table+"_"+threshold));
			HashSet<String> prerequisites = new HashSet<String>();
			String line1;
			while((line1=br1.readLine())!=null)
			{
				StringTokenizer tok = new StringTokenizer(line1,"\t");
				while(tok.hasMoreTokens())
				{
					String aa1 = tok.nextToken();
					if(aa1.equals(i))
					{
						continue;
					}
					else
					{
						prerequisites.add(aa1);
					}
				}
			}
			ArrayList<Edge> ee = new ArrayList<Edge>();
			for(String e:prerequisites)
			{
				ee.add(new Edge(0,e));
			}
			
			prerequisiteGraph.put(i, ee);
			
		}
		AdjList pr = new AdjList(prerequisiteGraph);
	//	LanguageModelEntity.assignLabelsBasedOnEntropy(entityMap, embedding1, embedding2, embedding3, input, outputfile, wiki)
	}
	
	
	
   public static void extractMetaPathsDatabase(String kb, String relation) throws Exception
   {
		Class.forName("org.postgresql.Driver");
		System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		Statement stmt = null;
		connection = DriverManager.getConnection(
				"jdbc:postgresql://127.0.0.1:5432/dbpedia", "prajna",
				"prajna");
		
		if (connection != null) 
		{
			System.out.println("You made it, take control your database now!");
			System.out.println("You made it, take control your database now!");
			ReadSubgraph rr = new ReadSubgraph();
			AdjList a = rr.readKB(new BufferedReader(new FileReader(kb)));
			System.out.println("read kb");
			AdjList app = a.returnTriplesOfName(relation);
			System.out.println("read application triples");
			System.out.println(app.numberOfEdges());
			for(String aa1:app.getAdjList().keySet())
			{
				System.out.println(aa1);
				ArrayList<Edge> e1 = app.getAdjList().get(aa1);
				for(Edge e:e1)
				{
					System.out.println(aa1+"\t"+e.getName()+"\n");
					String sql = "insert into paths1 (with dd as (select d1.arg1 as a1, d1.arg2 as a2, d1.arg3 as a3, d2.arg2 as a4, d2.arg3 as a5 from dbpedia_triples3 d1, dbpedia_triples3 d2 where d1.arg3 like d2.arg1) select dd.a1, dd.a2, dd.a3, dd.a4, dd.a5 from dd where dd.a1 like (?) and dd.a5 like (?))";
					System.out.println(sql);
					PreparedStatement statement= connection.prepareStatement(sql);
					//aa1=format(aa1);
					statement.setString(1, aa1);
					statement.setString(2, e.getName());
					statement.executeUpdate();
					statement.close();
				}
			}
		}
		
   }
   
   /**
	 * given a graph kb and a relation "relation", this generates meta-paths for the relation of length "length". The graph is stored in database. The paths are written in "outfile"
	 * @param relation
	 * @param outfile
	 * @param kb1: knowledge graph from where the triples for relation described by "relation" are extracted
	 * @param kb2: knowledge graph where we search for meta-paths between the entities involved in relation described by "relation" in kb1
	 * @param length
	 * @throws Exception
	 */
	
   
	public static void generateMetaPathsOfLengthDatabase(String relation, String outfile, String kb1, String table_name, int length) throws Exception
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		ReadSubgraph rr = new ReadSubgraph();
		AdjList a = rr.readKB(new BufferedReader(new FileReader(kb1)));
		AdjList app = a.returnTriplesOfName(relation);
		DBAdjList aa = new DBAdjList(table_name, "prajna", "prajna", "dbpedia");
		
		
		
		/*BufferedWriter bw2 = new BufferedWriter(new FileWriter("/home/prajna/tkb_with_inverses"));
		a1.writeToFile(bw2);
		bw2.close();
		*/
		
		ArrayList<Edge> eee = new ArrayList<Edge>();
		System.out.println("number of edges in: "+app.numberOfEdges());
		for(String aa1:app.getAdjList().keySet())
		{
			if(aa.retrieveNeighbours(aa1).size()==0) continue;
			ArrayList<Edge> e1 = app.getAdjList().get(aa1);
			for(Edge e:e1)
			{
				if(aa.retrieveNeighbours(e.getName()).size()==0) continue;
				System.out.println(aa1+"\t"+e.getName());
				aa.allKHopPaths(aa1, e.getName(),length, bw);
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
		Collections.sort(aa1,valueComparator);
		for(Entry<String, Integer> dd:aa1)
		{
			bw1.write(dd.getKey()+dd.getValue()+"\n");
		}
		bw1.close();
		
	}
	
	
	
	public static void replaceNullCharcaters(String kb, String outfile) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(kb));
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		String line;
		while((line=br.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			int c=0;
			while(tok.hasMoreTokens())
			{
				String s1 = tok.nextToken();
				if(c==0) 
				{
					if(s1.equals("1\0"))
					{
						System.out.println("here");
					}
					bw.write(s1.replaceAll("\u0000", ""));
				}
				else
				{
					if(s1.equals("1\0"))
					{
						System.out.println("here");
					}
					bw.write("\t"+s1.replaceAll("\u0000", ""));
				}
				c++;
			}
			bw.write("\n");
		}
		bw.close();
		
	}
	
	public static void insert_statement(String kb, String table) throws Exception
	{
		Class.forName("org.postgresql.Driver");
		System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		connection = DriverManager.getConnection(
				"jdbc:postgresql://127.0.0.1:5432/dbpedia", "prajna",
				"prajna");
		
		if (connection != null) 
		{
			System.out.println("Connected");
			BufferedReader br = new BufferedReader(new FileReader(kb));
			String line;
			while((line=br.readLine())!=null)
			{
				StringTokenizer tok = new StringTokenizer(line,"\t");
				ArrayList<String> s = new ArrayList<String>();
				while(tok.hasMoreTokens())
				{
					s.add(tok.nextToken());
				}
				String sql = "insert into "+table+" values (?,?,?)";
				PreparedStatement statement= connection.prepareStatement(sql);
				statement.setString(1, s.get(0));
				statement.setString(2, s.get(1));
				statement.setString(3, s.get(2));
				statement.executeUpdate();
				statement.close();
			}
			
		}
	}
	
	/**
	 * reads the 'file' that lists the path sequence and its frequency. Returns a HashMap of String to Double after reading the file
	 * @param file
	 * @return
	 * @throws Exception
	 */
	
	public static HashMap<String, Double> readFile(String file) throws Exception
	{
		HashMap<String, Double> counts = new HashMap<String, Double>();
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while((line=br.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> l = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				l.add(tok.nextToken());
			}
			String meta_path="";
			for(int i=0;i<l.size();i++)
			{
				if(i==l.size()-1)
				{
					
				}
				else
				{
					meta_path = meta_path + " "+ l.get(i);
				}
			}
			counts.put(meta_path, Double.parseDouble(l.get(l.size()-1)));
			
		}
		return counts;
	}
	
	public static void choose(String line, BufferedWriter bw, String app) throws Exception
	{
		StringTokenizer tok1 = new StringTokenizer(line,"\t");
		ArrayList<String> tokens1 = new ArrayList<String>();
		while(tok1.hasMoreTokens())
		{
			tokens1.add(tok1.nextToken());
		}
		
		StringTokenizer tok = new StringTokenizer(tokens1.get(0)," ");
		ArrayList<String> tokens = new ArrayList<String>();
		while(tok.hasMoreTokens())
		{
			tokens.add(tok.nextToken());
		}
		System.out.println(tokens.size());
		if((tokens.get(1).contains(app) || tokens.get(1).contains(app)) && Double.parseDouble(tokens1.get(1))>0)
		{
			bw.write(tokens.get(0)+" "+tokens.get(1)+"\n");
		}
	}
	
	public static void chooseExtraPaths(String parent_folder, String file) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(file));
		BufferedWriter bw1 = new BufferedWriter(new FileWriter(parent_folder+"/application/selected_meta-paths"));
		BufferedWriter bw2 = new BufferedWriter(new FileWriter(parent_folder+"/algorithm/selected_meta-paths"));
		BufferedWriter bw3 = new BufferedWriter(new FileWriter(parent_folder+"/implementation/selected_meta-paths"));
		String line;
		int c=0;
		int c2=0;
		while((line=br.readLine())!=null)
		{
			c=c+1;
			if(line.equals("\n")) continue;
			
			if(!line.startsWith(" "))
			{
				if(line.equals("application"))
				{
					c2=1;
				}
				else if(line.equals("algorithm"))
				{
					c2=2;
				}
				else if(line.equals("implementation"))
				{
					c2=3;
				}
			}
			else if(line.startsWith(" "))
			{
				if(c2==1) 
				{
					choose(line,bw1,"applicationof");
				}
				else if(c2==2) 
				{
					choose(line,bw2,"algorithmfor");
				}
				else if(c2==3) 
				{
					choose(line,bw3,"implementationof");
				}
				
			}
		}
		bw1.close();
		bw2.close();
		bw3.close();
		
	}
	/**
	 * 
	 * @param app
	 * @param tech
	 * @param algo
	 * @param impl
	 * @throws Exception
	 */
	
	public static void extractImportantPaths(String app, String outfile) throws Exception
	{
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		HashMap<String, Double> app_counts = readFile(app+"/meta-path-applicationof_length_2_with_counts.tsv");
		//HashMap<String, Double> tech_counts = readFile(app+"/meta-path-techniquein_length_2_with_counts.tsv");
		HashMap<String, Double> algo_counts = readFile(app+"/meta-path-algorithmfor_length_2_with_counts.tsv");
		HashMap<String, Double> impl_counts = readFile(app+"/meta-path-implementationof_length_2_with_counts.tsv");
		HashSet<String> h1 = new HashSet<>(app_counts.keySet());
		//h1.addAll(tech_counts.keySet());
		h1.addAll(algo_counts.keySet());
		h1.addAll(impl_counts.keySet());
		
		HashMap<String, Double> tf_idf_map_app = new HashMap<String, Double>();
		HashMap<String, Double> tf_idf_map_tech = new HashMap<String, Double>();
		HashMap<String, Double> tf_idf_map_algo = new HashMap<String, Double>();
		HashMap<String, Double> tf_idf_map_impl = new HashMap<String, Double>();
		for(String s:h1)
		{
			double tf1 = ((app_counts.get(s) == null) ? 0 : app_counts.get(s)) ;
			double idf1 = Math.log(4/(((app_counts.get(s) == null) ? 0 : 1) +  ((algo_counts.get(s) == null) ? 0 : 1) + ((impl_counts.get(s) == null) ? 0 : 1)));
			//double tf2 = (tech_counts.get(s)==null?0:tech_counts.get(s));
			
			double tf3 = (algo_counts.get(s)==null?0:algo_counts.get(s));
			
			double tf4 = (impl_counts.get(s)==null?0:impl_counts.get(s));
			
			if(tf1!=0) tf_idf_map_app.put(s, tf1*idf1);
			//if(tf2!=0) tf_idf_map_tech.put(s, tf2*idf1);
			if(tf3!=0) tf_idf_map_algo.put(s, tf3*idf1);
			if(tf4!=0) tf_idf_map_impl.put(s, tf4*idf1);
		}
		
		
		ArrayList<Entry<String, Double>> ee = new ArrayList<Entry<String, Double>>(tf_idf_map_app.entrySet());
		//ArrayList<Entry<String, Double>> ee1 = new ArrayList<Entry<String, Double>>(tf_idf_map_tech.entrySet());
		ArrayList<Entry<String, Double>> ee2 = new ArrayList<Entry<String, Double>>(tf_idf_map_algo.entrySet());
		ArrayList<Entry<String, Double>> ee3 = new ArrayList<Entry<String, Double>>(tf_idf_map_impl.entrySet());
		
		Collections.sort(ee, doubleComparator);
		//Collections.sort(ee1, doubleComparator);
		Collections.sort(ee2, doubleComparator);
		Collections.sort(ee3, doubleComparator);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		bw.write("\n\napplication\n\n");
		for(Entry<String, Double> e:ee)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}
		/*bw.write("\n\ntechnique\n\n");
		for(Entry<String, Double> e:ee1)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}*/
		bw.write("\n\nalgorithm\n\n");
		for(Entry<String, Double> e:ee2)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}
		bw.write("\n\nimplementation\n\n");
		for(Entry<String, Double> e:ee3)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}
		bw.close();
		//chooseExtraPaths(hm.get("parent-folder1"),outfile);
		
	}
	
	/**
	 * 
	 * @param app
	 * @param tech
	 * @param algo
	 * @param impl
	 * @throws Exception
	 */
	
	public static void extractImportantPaths1(String app, String outfile) throws Exception
	{
		HashMap<String, Double> app_counts = readFile(app+"/meta-path-applicationof_length_2_with_counts.tsv");
		//HashMap<String, Double> tech_counts = readFile(app+"/meta-path-techniquein_length_2_with_counts.tsv");
		HashMap<String, Double> algo_counts = readFile(app+"/meta-path-algorithmfor_length_2_with_counts.tsv");
		HashMap<String, Double> impl_counts = readFile(app+"/meta-path-implementationof_length_2_with_counts.tsv");
		HashSet<String> h1 = new HashSet<>(app_counts.keySet());
		//h1.addAll(tech_counts.keySet());
		h1.addAll(algo_counts.keySet());
		h1.addAll(impl_counts.keySet());
		int app_count = 19041;
		int tech_count = 6402;
		int algo_count = 5060;
		int impl_count = 9214;
		
		HashMap<String, Double> tf_idf_map_app = new HashMap<String, Double>();
		//HashMap<String, Double> tf_idf_map_tech = new HashMap<String, Double>();
		HashMap<String, Double> tf_idf_map_algo = new HashMap<String, Double>();
		HashMap<String, Double> tf_idf_map_impl = new HashMap<String, Double>();
		for(String s:h1)
		{
			double tf1 = ((app_counts.get(s) == null) ? 0 : app_counts.get(s)) ;
			double idf1 = 3/(((app_counts.get(s) == null) ? 0 : (app_counts.get(s)/app_count)) +  ((algo_counts.get(s) == null) ? 0 : (algo_counts.get(s)/algo_count)) + ((impl_counts.get(s) == null) ? 0 : (impl_counts.get(s)/impl_count)));
			//double tf2 = (tech_counts.get(s)==null?0:tech_counts.get(s));
			
			double tf3 = (algo_counts.get(s)==null?0:algo_counts.get(s));
			
			double tf4 = (impl_counts.get(s)==null?0:impl_counts.get(s));
			
			if(tf1!=0) tf_idf_map_app.put(s, tf1*idf1);
			//if(tf2!=0) tf_idf_map_tech.put(s, tf2*idf1);
			if(tf3!=0) tf_idf_map_algo.put(s, tf3*idf1);
			if(tf4!=0) tf_idf_map_impl.put(s, tf4*idf1);
			
			/*tf_idf_map_app.put(s, idf1);
			tf_idf_map_tech.put(s, idf1);
			tf_idf_map_algo.put(s, idf1);
			tf_idf_map_impl.put(s, idf1);*/
		}
		
		
		ArrayList<Entry<String, Double>> ee = new ArrayList<Entry<String, Double>>(tf_idf_map_app.entrySet());
		//ArrayList<Entry<String, Double>> ee1 = new ArrayList<Entry<String, Double>>(tf_idf_map_tech.entrySet());
		ArrayList<Entry<String, Double>> ee2 = new ArrayList<Entry<String, Double>>(tf_idf_map_algo.entrySet());
		ArrayList<Entry<String, Double>> ee3 = new ArrayList<Entry<String, Double>>(tf_idf_map_impl.entrySet());
		
		Collections.sort(ee, doubleComparator);
	//	Collections.sort(ee1, doubleComparator);
		Collections.sort(ee2, doubleComparator);
		Collections.sort(ee3, doubleComparator);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		bw.write("\n\napplication\n\n");
		for(Entry<String, Double> e:ee)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}
	/*	bw.write("\n\ntechnique\n\n");
		for(Entry<String, Double> e:ee1)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}*/
		bw.write("\n\nalgorithm\n\n");
		for(Entry<String, Double> e:ee2)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}
		bw.write("\n\nimplementation\n\n");
		for(Entry<String, Double> e:ee3)
		{
			bw.write(e.getKey()+"\t"+e.getValue()+"\n");
		}
		bw.close();
		
	}
	
	/*public static void readGraphEfficient(String kb) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(kb));
		Object2ObjectLinkedOpenHashMap<String, ObjectArrayList<String>> h= new Object2ObjectLinkedOpenHashMap<String, ObjectArrayList<String>>(100000000,(float) 0.3);
		
		String line;
		int count=0;
		while((line=br.readLine())!=null)
		{
			count++;
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> tokens = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				tokens.add(tok.nextToken());
			}
			if(tokens.size()!=3) continue;
			ObjectArrayList<String> o;
			if(h.get(tokens.get(0))!=null)
			{
				o = h.get(tokens.get(0));
			}
			else
			{
				o = new ObjectArrayList<String>();
			}
			o.add(tokens.get(2));
			h.put(tokens.get(0), o);
			if(count==100000000) break;
			tokens=null;
			//java.lang.Runtime.getRuntime().gc();
		}
		
		System.out.println(h.size());
	}*/
	
	
	
	
	/**
	 * efficient way to read a graph. Uses Google Guava library to represent the graph compactly
	 * @param kb
	 * @return
	 * @throws Exception
	 */
	
	public static AdjListCompact readGraphEfficientAlternate(String kb, String relMap, String nodeMap) throws Exception
	{
		
		MutableValueGraph<Integer, ArrayList<Integer>> weightedGraph = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		HashMap<Integer, Integer> h1 = new HashMap<Integer, Integer>();
		
		BufferedReader br = new BufferedReader(new FileReader(kb));
		String line;
		
		while((line=br.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> tokens = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				tokens.add(tok.nextToken());
			}
			if(tokens.size()!=3) continue;
			Integer a = (Integer) Integer.parseInt(tokens.get(0));
			Integer b = (Integer) Integer.parseInt(tokens.get(2));
			Optional c1 =  weightedGraph.edgeValue(a, b);
			if(c1.isPresent())
			{
				ArrayList<Integer> c = (ArrayList<Integer>) c1.get();
				c.add(Integer.parseInt(tokens.get(1)));
				weightedGraph.putEdgeValue(a, b, c);
			}
			else
			{
				ArrayList<Integer> c = new ArrayList<Integer>();
				c.add(Integer.parseInt(tokens.get(1)));
				weightedGraph.putEdgeValue(a, b, c);
			}
			
			
			
		}
		HashMap<String, Integer> relmap = new HashMap<String, Integer>();
		HashMap<Integer, String> relmap1 = new HashMap<Integer, String>();
		BufferedReader br1 = new BufferedReader(new FileReader(relMap));
		while((line=br1.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> tokens = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				tokens.add(tok.nextToken());
			}
			if(tokens.size()!=2) continue;
			relmap.put(tokens.get(0), Integer.parseInt(tokens.get(1)));
			relmap1.put(Integer.parseInt(tokens.get(1)), tokens.get(0));
		}
		
		for(String s:relmap.keySet())
		{
			for(String s1:relmap.keySet())
			{
				if(s.equals(s1)) continue;
				if((s1.endsWith("_inverse") && s1.replace("_inverse", "").equals(s)) || (s.endsWith("_inverse") && s.replace("_inverse", "").equals(s1)))
				{
					h1.put(relmap.get(s), relmap.get(s1));
					h1.put(relmap.get(s1), relmap.get(s));
				}
			}
		}
		
		HashMap<Integer, String> nodeIndex = new HashMap<Integer, String>();
		HashMap<String, Integer> nodeIndex1 = new HashMap<String, Integer>();
		BufferedReader br2 = new BufferedReader(new FileReader(nodeMap));
		while((line=br2.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> tokens = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				tokens.add(tok.nextToken());
			}
			if(tokens.size()!=2) continue;
			nodeIndex.put(Integer.parseInt(tokens.get(1)), tokens.get(0));
			nodeIndex1.put(tokens.get(0), Integer.parseInt(tokens.get(1)));
		}
	
		AdjListCompact aa = new AdjListCompact(ImmutableValueGraph.copyOf(weightedGraph));
		aa.setInverses(h1);
		aa.setRelMap(relmap);
		aa.nodeMap=nodeIndex;
		aa.nodeMap1=nodeIndex1;
		aa.relmap1 = relmap1;
		 //Runtime r = Runtime.getRuntime();
		//r.gc();
		return aa;
		
	}
	
	/**
	 * older code to read the graph efficient way to read a graph. Uses Google Guava library to represent the graph compactly
	 * @param kb
	 * @return
	 * @throws Exception
	 */
	
	public static AdjListCompactOld readGraphEfficient(String kb) throws Exception
	{
		
		MutableValueGraph<String, String> weightedGraph = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		
		BufferedReader br = new BufferedReader(new FileReader(kb));
		String line;
		
		while((line=br.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> tokens = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				tokens.add(tok.nextToken());
			}
			if(tokens.size()!=3) continue;
			weightedGraph.putEdgeValue(tokens.get(0), tokens.get(2), tokens.get(1));
			
			//tok=null;
			//tokens=null;
			
		}
		 //Runtime r = Runtime.getRuntime();
		//r.gc();
		return new AdjListCompactOld(weightedGraph);
		
	}
	
	/**
	 * reading the graph without creating the supporting data structures
	 * @param kb
	 * @return
	 * @throws Exception
	 */
	
	public static AdjListCompact readGraphEfficient1(String kb) throws Exception
	{
		
		MutableValueGraph<Integer, ArrayList<Integer>> weightedGraph = ValueGraphBuilder.directed().allowsSelfLoops(true).build();
		
		BufferedReader br = new BufferedReader(new FileReader(kb));
		String line;
		
		while((line=br.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> tokens = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				tokens.add(tok.nextToken());
			}
			if(tokens.size()!=3) continue;
			Integer a = (Integer) Integer.parseInt(tokens.get(0));
			Integer b = (Integer) Integer.parseInt(tokens.get(2));
			Optional c1 =  weightedGraph.edgeValue(a, b);
			if(c1.isPresent())
			{
				ArrayList<Integer> c = (ArrayList<Integer>) c1.get();
				c.add(Integer.parseInt(tokens.get(1)));
				weightedGraph.putEdgeValue(a, b, c);
			}
			else
			{
				ArrayList<Integer> c = new ArrayList<Integer>();
				c.add(Integer.parseInt(tokens.get(1)));
				weightedGraph.putEdgeValue(a, b, c);
			}
			
		}
		
		 //Runtime r = Runtime.getRuntime();
		//r.gc();
		return new AdjListCompact(ImmutableValueGraph.copyOf(weightedGraph));
		
	}
	
	/**
	 * reads the selected meta paths listed inside folder2 and returns a list of lists
	 * @param folder1: grandparent folder
	 * @param folder2: parent folder
	 * @return
	 * @throws Exception
	 */
	
	public ArrayList<ArrayList<String>> readMetaPathList(String folder1, String folder2) throws Exception
	{
		BufferedReader br1 = new BufferedReader(new FileReader(folder1+"/"+folder2+"/selected_meta-paths"));
		String line;
		ArrayList<ArrayList<String>> list1 = new ArrayList<ArrayList<String>>();
		while((line=br1.readLine())!=null)
		{
			ArrayList<String> list = new ArrayList<String>();
			StringTokenizer tok = new StringTokenizer(line," ");
			while(tok.hasMoreTokens())
			{
				list.add(tok.nextToken());
			}
			list1.add(list);
		}
		return list1;
	}
	
	/**
	 * for an index j, instead of random walk, it assigns the array values based on the expectation of visiting the neighbours of node n
	 * @param j
	 * @param node
	 * @param a
	 * @param rws
	 */
	
	public void estimateNeighborsAtIndex(int j, int node, AdjListCompact a, int[][] rws, HashMap<Integer, ArrayList<Integer>> pathGraph)
	{
		ArrayList<Integer> s1 = pathGraph.get(node);
		//ArrayList<Integer> ll = new ArrayList<Integer>(s1);
		if(s1.size()==0) return;
		int n1 = 1000/s1.size();
		int n2 = 1000 % s1.size();
		int temp=0;
		for(int nn=0;nn<s1.size();nn++)
		{
			int cc = s1.get(nn);
			for(int i=0;i<n1;i++)
			{
				temp = nn*n1 + i;
				rws[temp][j]=cc;
			}
		}
		Random rr = new Random();
		for(int i=temp+1;i<=temp+n2;i++)
		{
			int r = rr.nextInt(s1.size());
			rws[temp][j]=s1.get(r);
		}
		
	}
	
	
	
	/**
	 * estimates nodes at step j given the source neighbours at step j-1
	 * @param j
	 * @param source
	 * @param a
	 * @param rws
	 * @param pathgraph
	 */
	
	public void estimateNeighborsAtIndex1(int j, HashMap<Integer, ArrayList<Integer>> source, AdjListCompact a, int[][] rws, HashMap<Integer, ArrayList<Integer>> pathgraph)
	{
		for(int n:source.keySet())
		{
			ArrayList<Integer> ll = pathgraph.get(n);
			//ArrayList<Integer> ll = new ArrayList<Integer>(s1);
			if(ll.size()==0) return;
			int n1 = source.get(n).size()/ll.size();
			int n2 = source.get(n).size() % ll.size();
			int temp=0;
			for(int nn=0;nn<ll.size();nn++)
			{
				int cc = ll.get(nn);
				for(int i=0;i<n1;i++)
				{
					temp = source.get(n).get((nn*n1)+i);
					rws[temp][j]=cc;
				}
			}
			Random rr = new Random();
			for(int i=n1*ll.size();i<source.get(n).size();i++)
			{
				int r = rr.nextInt(ll.size());
				temp = source.get(n).get(i);
				rws[temp][j]=ll.get(r);
			} 
			
		}
	}
	
	/**
	 * reusing random numbers. generates 100*100*1000 random numbers and then re-uses the stream
	 * @param kb
	 * @param outfile
	 * @param folder
	 * @param relmap
	 * @throws Exception
	 */
	
	public void randomWalkMaster5(String kb, String outfile, String folder, String relmap, String nodeMap) throws Exception
	{
		long startTime = System.nanoTime();
		//long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		AdjListCompact a = readGraphEfficientAlternate(kb, relmap, nodeMap);
		long endTime = System.nanoTime();
		System.out.println("reading graph done, time taken: "+(endTime-startTime)/1000000000);
		ArrayList<Integer> path = new ArrayList<Integer>();
		path.add(4);
		path.add(12);
		
		a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path));
		a.createIndexPathMasterAlternate(path);
		
		long endTime2 = System.nanoTime();
		System.out.println("path adjacency list made, time taken = "+(endTime2 - endTime)/1000000000);
		int rws[][] = new int[100000][100];
		for(int i=0;i<100;i++)
		{
			for(int j=0;j<1000;j++)
			{
				for(int k=0;k<100;k++)
				{
					
				}
			}
		}
	}
	
	/**
	 * computes the 1000*100 matrix by round-robin fashion. for each node, maintains an index of the last element chosen from its set of neighbors. increments it modulo the size of its neighbors at each step 
	 * @param kb
	 * @param outfile
	 * @param folder
	 * @param relmap
	 * @throws Exception
	 */
	
	public void randomWalkMasterHPC4(String kb, String outfile, String folder, String relmap, String nodeMap) throws Exception
	{
		long startTime = System.nanoTime();
		//long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		AdjListCompact a = readGraphEfficientAlternate(kb, relmap, nodeMap);
		long endTime = System.nanoTime();
		System.out.println("reading graph done, time taken: "+(endTime-startTime)/1000000000);
		ArrayList<Integer> path = new ArrayList<Integer>();
		path.add(4);
		path.add(12);
		
		a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path));
		a.createIndexPathMasterAlternate(path);
		
		//HashMap<Integer, ArrayList<Integer>> h1 = new HashMap<Integer, ArrayList<Integer>>();
		
		long endTime2 = System.nanoTime();
		System.out.println("path adjacency list made, time taken = "+(endTime2 - endTime)/1000000000);
		
		int count=0;
		System.out.println(a.pathGraph.nodes().size());
	
		ArrayList<Integer> aa = new ArrayList<Integer>();
		for(int i=0;i<100;i++)
		{
			aa.add(i);
		}
		for(int n:a.pathGraph.nodes())
		{
			int[][] rws = new int[1000][100];
			Set<Integer> s = a.pathGraph.successors(n);
			if(s.size()==0) continue;
			for(int i=0;i<1000;i++)
			{
				rws[i][0]=n;
			}
			count++;
			if(count%100==0)
			{
				System.out.println(count);
			}
			HashMap<Integer, Integer> lastIndex = new HashMap<Integer, Integer>();
			Random rr = new Random();
			for(int ii=0;ii<1000;ii++)
			{
				for(int jj=0;jj<99;jj++)
				{
					ArrayList<Integer> s1;
					if(jj % 2==0)
					{
						s1 = a.pathGraphHashmap.get(rws[ii][jj]);
					}
					else
					{
						s1 = a.pathGraphHashMapReverse.get(rws[ii][jj]);
					}
					if(lastIndex.get(rws[ii][jj])==null)
					{
						rws[ii][jj+1]=s1.get(0);
						lastIndex.put(rws[ii][jj], 0);
					}
					else
					{
						int ind = (lastIndex.get(rws[ii][jj])+1) % s1.size();
						rws[ii][jj+1] = s1.get(ind);
						lastIndex.put(rws[ii][jj], ind);
					}
					
					//rws[ii][jj+1]= s1.get(rr.nextInt(s1.size()));
				}
			}
		}
		
	}
	
	/**
	 * this module estimates the step 2 neighbors of the randomwalk and does actual random walk for the remaining steps
	 * @param kb
	 * @param outfile
	 * @param folder
	 * @param relmap
	 * @throws Exception
	 */
	

	public void randomWalkMasterHPC3(String kb, String outfile, String folder, String relmap, String nodeMap) throws Exception
	{
		long startTime = System.nanoTime();
		//long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		AdjListCompact a = readGraphEfficientAlternate(kb, relmap, nodeMap);
		long endTime = System.nanoTime();
		System.out.println("reading graph done, time taken: "+(endTime-startTime)/1000000000);
		ArrayList<Integer> path = new ArrayList<Integer>();
		path.add(1);
		path.add(36);
		
		a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path));
		a.createIndexPathMasterAlternate(path);
		
		long endTime2 = System.nanoTime();
		System.out.println("path adjacency list made, time taken = "+(endTime2 - endTime)/1000000000);
		
		int count=0;
		System.out.println(a.pathGraph.nodes().size());
		
		for(int n:a.pathGraph.nodes())
		{
			int[][] rws = new int[1000][100];
			ArrayList<Integer> s = a.pathGraphHashmap.get(n);
			if(s.size()==0) continue;
			for(int i=0;i<1000;i++)
			{
				rws[i][0]=n;
			}
			count++;
			if(count%100==0)
			{
				System.out.println(count);
			}
			HashMap<Integer, ArrayList<ArrayList<Integer>>> randomwalk = new HashMap<Integer, ArrayList<ArrayList<Integer>>>();
			estimateNeighborsAtIndex(1,n,a,rws,a.pathGraphHashmap);
			int num_threads = Runtime.getRuntime().availableProcessors(); 
			int nn = 1000;
			ThreadPoolExecutor executor = new ThreadPoolExecutor(num_threads,
					nn, Long.MAX_VALUE, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(nn));
			
			/*for(int num=0;num<1000;num++)
			{
				if(a.pathGraphHashMapReverse.get(rws[num][1]).size()==0) continue;
				TaskRandomSelect t1 = new TaskRandomSelect(a, 100, randomwalk, rws[num][1], n, 2);
				executor.execute(t1);
			}
			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
			int ii=0;*/
			/*for(ArrayList<Integer> aa:randomwalk.get(n))
			{
				int jj=2;
				for(int aa1:aa)
				{
					rws[ii][jj]=aa1;
					jj++;
				}
				ii++;
			}*/
		}
	
		
	}
	
	/**
	 * test module to check code on hpc. Here the random number generation is replaced by estimating the nodes at a step given a starting node. Not complete
	 * @param kb: knowledge graph used for meta-path2vec
	 * @param outfile
	 * @param folder
	 * @param relmap
	 * @throws Exception
	 */
	
	public void randomWalkMasterHPC1(String kb, String outfile, String folder, String relmap, String nodeMap) throws Exception
	{
		long startTime = System.nanoTime();
		//long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		AdjListCompact a = readGraphEfficientAlternate(kb, relmap, nodeMap);
		long endTime = System.nanoTime();
		System.out.println("reading graph done, time taken: "+(endTime-startTime)/1000000000);
		ArrayList<Integer> path = new ArrayList<Integer>();
		path.add(4);
		path.add(12);
		
		a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path));
		
		a.createIndexPathMasterAlternate(path);
		
		long endTime2 = System.nanoTime();
		System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
		//BufferedWriter bw = new BufferedWriter(new FileWriter("/home/cse/phd/csz138110/scratch/dbpedia/test/random_walk_4"));
	//	Random r = new Random();
		int count=0;
		System.out.println(a.pathGraph.nodes().size());
		int num_threads = Runtime.getRuntime().availableProcessors(); 
		int nn = a.pathGraph.nodes().size();
		ThreadPoolExecutor executor = new ThreadPoolExecutor(num_threads,
				nn, Long.MAX_VALUE, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(nn));
		for(int n:a.pathGraph.nodes())
		{
			int[][] rws = new int[1000][100];
			ArrayList<Integer> ll = a.pathGraphHashmap.get(n);
			//ArrayList<Integer> ll = new ArrayList<Integer>(s);
			if(ll==null) continue;
			if(ll.size()==0) continue;
			
			/*for(int[] i:rws)
			{
				for(int j:i)
				{
					bw.write(j+" ");
				}
				bw.write("\n");
			}*/
				
		}
		//bw.close();
		
	}

	
	/**
	 * test module for random walks where they are parallelized across nodes in a graph instead of parallelizing the 'numwalks' walks
	 * @param kb
	 * @param outfile
	 * @param folder
	 * @param relmap
	 * @throws Exception
	 */
	
	public void randomWalkMasterHPC2(String kb, String outfile, String folder, String relmap, String nodeMap) throws Exception
	{
		long startTime = System.nanoTime();
		//long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		AdjListCompact a = readGraphEfficientAlternate(kb, relmap, nodeMap);
		long endTime = System.nanoTime();
		System.out.println("reading graph done, time taken: "+(endTime-startTime)/1000000000);
		ArrayList<Integer> path = new ArrayList<Integer>();
		path.add(1);
		path.add(36);
		a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path));
		
		a.createIndexPathMasterAlternate(path);
		
		long endTime2 = System.nanoTime();
		System.out.println("indexes on meta-path created, time taken = "+(endTime2 - endTime)/1000000000);
		a.generateRandomWalksAlternate(1000, 100);
	}
	
	
	
	
	/**
	 * test module to check the parallelized version of random walks which selects each neighbor randomly. Uses the meta-path adjacency list to choose a neighbor, instaed of selecting a neighbor related via each relation of meta-path
	 * @param kb
	 * @param outfile
	 * @param folder
	 * @param relmap
	 * @throws Exception
	 */
	
	public void randomWalkMasterHPC(String kb, String outfile, String folder, String relmap, String relation, String nodeMap) throws Exception
	{
		long startTime = System.nanoTime();
		//long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		AdjListCompact a = readGraphEfficientAlternate(kb, relmap, nodeMap);
		//a.writeToFile("/home/cse/phd/csz138110/scratch/teknowbase_alternate/test.tsv");
		//System.out.println(a.labeledGraph.edges().size());
		long endTime = System.nanoTime();
		System.out.println("reading graph done, time taken: "+(endTime-startTime)/1000000000);
		//ArrayList<ArrayList<String>> application = this.readMetaPathList(folder, relation);
	/*	ArrayList<ArrayList<String>> algorithm = this.readMetaPathList(folder, "algorithm");
		ArrayList<ArrayList<String>> technique = this.readMetaPathList(folder, "technique");
		ArrayList<ArrayList<String>> implementation = this.readMetaPathList(folder, "implementation");
		
		BufferedWriter bw2 = new BufferedWriter(new FileWriter(outfile+"/algorithm.txt"));
		BufferedWriter bw3 = new BufferedWriter(new FileWriter(outfile+"/technique.txt"));
		BufferedWriter bw4 = new BufferedWriter(new FileWriter(outfile+"/implementation.txt"));*/
		BufferedWriter bw1 = new BufferedWriter(new FileWriter(outfile+"/"+relation.replace(" ", "-")+".txt"));
		StringTokenizer tok = new StringTokenizer(relation," ");
		ArrayList<String> as = new ArrayList<String>();
		while(tok.hasMoreTokens())
		{
			as.add(tok.nextToken());
		}
		//for(ArrayList<String> as:application)
		//{
			//System.out.println("reading graph for"+ relation+" done, time taken to read graph = "+(endTime - startTime)/1000000000);
			ArrayList<Integer> path1 = new ArrayList<Integer>();
			int flag=0;
			for(String ss:as)
			{
				System.out.println(ss+"\t");
				if(a.relmap.get(ss)==null) 
				{
					System.out.println(ss+" has no mapping");
					flag=1;
					break;
				}
				
				path1.add(a.relmap.get(ss));
			}
			if(flag==1) return;
			System.out.println("\n");
			if(path1.size()!=as.size())
			{
				System.out.println("sizes are not the same");
				return;
			}
			else
			{
				System.out.println(path1.get(0)+"\t"+path1.get(1));
			}
			a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path1));
			
			a.createIndexPathMasterAlternate(path1);
			a.pairAdjList=null;
			a.pathGraph=null;
			a.pathGraph_inverse=null;
			System.gc();
			a.generateRandomWalksLatest(bw1, 100, 1000);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			
		//}
		bw1.close();
		/*for(ArrayList<String> as:algorithm)
		{
			System.out.println("reading graph for algorithm done, time taken to read graph = "+(endTime - startTime)/1000000000);
			ArrayList<Integer> path1 = new ArrayList<Integer>();
			for(String ss:as)
			{
				path1.add(a.relmap.get(ss));
			}
			HashMap<Long, Set<Integer>> h = a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path1));
			
			a.createIndexPathMasterAlternate(path1);
			a.generateRandomWalksLatest(bw2, 100, 1000);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			
		}
		bw2.close();
		/*for(ArrayList<String> as:technique)
		{
			System.out.println("reading graph for technique done, time taken to read graph = "+(endTime - startTime)/1000000000);
			
			ArrayList<Integer> path1 = new ArrayList<Integer>();
			for(String ss:as)
			{
				path1.add(a.relmap.get(ss));
			}
			HashMap<Long, Set<Integer>> h = a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path1));
			
			a.createIndexPathMasterAlternate(path1);
			a.generateRandomWalksLatest(bw3, 100, 1000);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			
		}
		bw3.close();
		for(ArrayList<String> as:implementation)
		{
			System.out.println("reading graph for implementation done, time taken to read graph = "+(endTime - startTime)/1000000000);
			
			ArrayList<Integer> path1 = new ArrayList<Integer>();
			for(String ss:as)
			{
				path1.add(a.relmap.get(ss));
			}
			HashMap<Long, Set<Integer>> h = a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path1));
			
			a.createIndexPathMasterAlternate(path1);
			a.generateRandomWalksLatest(bw4, 100, 1000);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			
		}
		
		bw4.close();*/
		
		//long endTime2 = System.nanoTime();
		System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
		
		//BufferedWriter bw = new BufferedWriter(new FileWriter("/mnt/dell/prajna/data/dbpedia/test/random_walk_4"));
	//	Random r = new Random();
		
		/*ArrayList<ArrayList<String>> application = this.readMetaPathList(folder, "application");
		ArrayList<ArrayList<String>> algorithm = this.readMetaPathList(folder, "algorithm");
		ArrayList<ArrayList<String>> technique = this.readMetaPathList(folder, "technique");
		ArrayList<ArrayList<String>> implementation = this.readMetaPathList(folder, "implementation");
		BufferedWriter bw1 = new BufferedWriter(new FileWriter(outfile+"/implmentation.txt"));
		for(ArrayList<String> as:implementation)
		{
			System.out.println("reading graph done, time taken to read graph = "+(endTime - startTime)/1000000000);
			ArrayList<String> path = as;
			ArrayList<Integer> path1 = new ArrayList<Integer>();
			for(String s1:path)
			{
				path1.add(a.relmap.get(s1));
			}
			//path.add("owl#sameas_inverse");
			//path.add("type");
			//System.out.println("number of nodes is "+a.labeledGraph.nodes().size());
			//Runtime.getRuntime().gc();
			a.generateRandomWalks(path1, 1000, 100, bw1,h);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			long afterUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
			long actualMemUsed = afterUsedMem - beforeUsedMem;
			System.out.println(actualMemUsed/(1024*1024)+" GB");
		}
		bw1.close();*/
		
	}
	
	public void randomWalkMaster1(String kb, String outfile, String folder, String relmap, String nodeMap) throws Exception
	{
		long startTime = System.nanoTime();
		AdjListCompact a = readGraphEfficientAlternate(kb, relmap, nodeMap);
		long endTime = System.nanoTime();
		System.out.println("reading graph done, time taken: "+(endTime-startTime)/1000000000);
		ArrayList<Integer> path = new ArrayList<Integer>();
		path.add(13);
		path.add(0);
		
		a.getListOfNeighborsForEdgeLabel(new HashSet<Integer>(path));
		
		BufferedWriter bw4 = new BufferedWriter(new FileWriter(outfile+"/implementation.txt"));
		
		System.out.println("reading graph done, time taken to read graph = "+(endTime - startTime)/1000000000);
		
	//	path.add("owl#sameas_inverse");
	//	path.add("type");
		System.out.println("number of nodes is "+a.labeledGraph.nodes().size());
		//Runtime.getRuntime().gc();
		a.generateRandomWalks(path, 1000, 100, bw4,a.pairAdjList);
		long endTime2 = System.nanoTime();
		System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
		bw4.close();

	}
	
	public static void generateMetaPathsOfLengthMaster1(String relation, String outfile, String kb1, String kb2, int length) throws Exception
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		long startTime = System.nanoTime();
		AdjListCompactOld a1 = readGraphEfficient(kb1);
		long endTime = System.nanoTime();
		System.out.println("read first graph, time taken ="+(endTime - startTime)/1000000000);
		AdjListCompactOld a2 = readGraphEfficient(kb2);
		long endTime1 = System.nanoTime();
		System.out.println("read second graph, time taken ="+(endTime1 - endTime)/1000000000);
		
		
	}
	/**
	 * 
	 * @param relation
	 * @param outfile
	 * @param kb1
	 * @param kb2
	 * @param length
	 * @param relmap1
	 * @param relmap2
	 * @param nodeMap1
	 * @param nodeMap2
	 * @throws Exception
	 */
	
	public static void generateMetaPathsOfLengthMaster(String relation, String outfile, String kb1, String kb2, int length, String relmap1, String relmap2, String nodeMap1, String nodeMap2) throws Exception
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		AdjListCompact a1 = readGraphEfficientAlternate(kb1, relmap1, nodeMap1);
		System.out.println("read first graph");
		AdjListCompact a2 = readGraphEfficientAlternate(kb2, relmap2, nodeMap2);
		System.out.println("read second graph");
		int r = a1.relmap.get(relation);
		a1.returnTriplesForEdgeLabel(r);
		System.out.println("retrieved pairs for relation");
		Set<EndpointPair> pair = a1.relIndex.get(r);
		int c1=0;
		for(EndpointPair p:pair)
		{
			c1++;
			int u = (int) p.nodeU();
			int v = (int) p.nodeV();
			if (a2.nodeMap1.get(a1.nodeMap.get(u))!=null && a2.nodeMap1.get(a1.nodeMap.get(v))!=null)
			{
				a2.findPaths(a2.nodeMap1.get(a1.nodeMap.get(u)), a2.nodeMap1.get(a1.nodeMap.get(v)), length, bw);
			}
			System.out.println(c1);
		}
		bw.close();
		System.out.println("found paths");
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
		Collections.sort(aa1,valueComparator);
		for(Entry<String, Integer> dd:aa1)
		{
			bw1.write(dd.getKey()+dd.getValue()+"\n");
		}
		bw1.close();
		
	}
	
	/**
	 * test module to do random walks on TeKnowbase
	 * @param kb
	 * @param outfile
	 * @param folder
	 * @throws Exception
	 */
	
	public void randomWalkMaster(String kb, String outfile, String folder) throws Exception
	{
		long startTime = System.nanoTime();
		long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		AdjListCompactOld a = readGraphEfficient(kb);
		long endTime = System.nanoTime();
		HashMap<String, HashMap<String, Set<String>>> h = a.getListOfNeighborsForEdgeLabel();
		ArrayList<ArrayList<String>> application = this.readMetaPathList(folder, "application");
		ArrayList<ArrayList<String>> algorithm = this.readMetaPathList(folder, "algorithm");
		ArrayList<ArrayList<String>> technique = this.readMetaPathList(folder, "technique");
		ArrayList<ArrayList<String>> implementation = this.readMetaPathList(folder, "implementation");
		BufferedWriter bw1 = new BufferedWriter(new FileWriter(outfile+"/application.txt"));
		BufferedWriter bw2 = new BufferedWriter(new FileWriter(outfile+"/algorithm.txt"));
		BufferedWriter bw3 = new BufferedWriter(new FileWriter(outfile+"/technique.txt"));
		BufferedWriter bw4 = new BufferedWriter(new FileWriter(outfile+"/implementation.txt"));
		/*for(ArrayList<String> as:application)
		{
			System.out.println("reading graph done, time taken to read graph = "+(endTime - startTime)/1000000000);
			ArrayList<String> path = as;
		//	path.add("owl#sameas_inverse");
		//	path.add("type");
			//System.out.println("number of nodes is "+a.labeledGraph.nodes().size());
			//Runtime.getRuntime().gc();
			a.generateRandomWalks(path, 1000, 100, bw1, h);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			long afterUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
			long actualMemUsed = afterUsedMem - beforeUsedMem;
			System.out.println(actualMemUsed/(1024*1024)+" GB");
		}
		for(ArrayList<String> as:algorithm)
		{
			System.out.println("reading graph done, time taken to read graph = "+(endTime - startTime)/1000000000);
			ArrayList<String> path = as;
		//	path.add("owl#sameas_inverse");
		//	path.add("type");
			//System.out.println("number of nodes is "+a.labeledGraph.nodes().size());
			//Runtime.getRuntime().gc();
			a.generateRandomWalks(path, 1000, 100, bw2,h);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			long afterUsedMem = Runtime.getRuntime().totalMemory() -  Runtime.getRuntime().freeMemory();
			long actualMemUsed = afterUsedMem - beforeUsedMem;
			System.out.println(actualMemUsed/(1024*1024)+" GB");
		}
		for(ArrayList<String> as:technique)
		{
			System.out.println("reading graph done, time taken to read graph = "+(endTime - startTime)/1000000000);
			ArrayList<String> path = as;
		//	path.add("owl#sameas_inverse");
		//	path.add("type");
			//System.out.println("number of nodes is "+a.labeledGraph.nodes().size());
			//Runtime.getRuntime().gc();
			a.generateRandomWalks(path, 1000, 100, bw3,h);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			long afterUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
			long actualMemUsed = afterUsedMem - beforeUsedMem;
			System.out.println(actualMemUsed/(1024*1024)+" GB");
		}*/
		for(ArrayList<String> as:implementation)
		{
			System.out.println("reading graph done, time taken to read graph = "+(endTime - startTime)/1000000000);
			ArrayList<String> path = as;
		//	path.add("owl#sameas_inverse");
		//	path.add("type");
			//System.out.println("number of nodes is "+a.labeledGraph.nodes().size());
			//Runtime.getRuntime().gc();
			a.generateRandomWalks(path, 1000, 100, bw4, h);
			long endTime2 = System.nanoTime();
			System.out.println("random walks for a path done, time taken to generate labels = "+(endTime2 - endTime)/1000000000);
			long afterUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
			long actualMemUsed = afterUsedMem - beforeUsedMem;
			System.out.println(actualMemUsed/(1024*1024)+" GB");
		}
		bw1.close();
		bw2.close();
		bw3.close();
		bw4.close();
	}
	
	
	
	/**
	 * adds triples from the knowledge graph represented by this class to kb represented by "kb" if both the entities participating in the triple belong to teknowbase
	 * @param kb
	 */
	
	
	
	
	public static void main(String args[]) throws Exception
	{
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		Evaluation e = new Evaluation();
		//e.connect("evaluation", "root", "admin", "tkbforir_evaluation");
		//e.getPapersForAllQueries(hm.get("query-file"), hm.get("parent-folder"), hm.get("heuristics"));
		Aspect a = new Aspect();
		a.generateMetaPathsOfLengthMaster(hm.get("relation"), hm.get("outfile"), hm.get("teknowbase"), hm.get("freebase"), 2, hm.get("teknowbase-relmap"), hm.get("freebase-relmap"), hm.get("teknowbase-nodes"), hm.get("freebase-nodes"));
		//AdjListCompactOld a1 = a.readGraphEfficient(hm.get("teknowbase"));
		//DBAdjList d = new DBAdjList("evaluation","tkbforir","123456","tkbforir_evaluation");
		//a1.addTriplesAnotherKB(hm.get("dbpedia-original"), hm.get("outfile"));
		//a.randomWalkMasterHPC(hm.get("yago"), hm.get("outfile"), hm.get("parent-folder"), hm.get("yago-relmap"), hm.get("relation"));
		//a.randomWalkMaster(hm.get("dbpedia"), hm.get("outfile"), hm.get("parent-folder"));
		//readGraphEfficient(hm.get("dbpedia"));
		//extractImportantPaths1(hm.get("parent-folder"), hm.get("outfile"));
		//insert_statement(hm.get("freebase-postprocessed"),"freebase_facts");
		//replaceNullCharcaters(hm.get("freebase"),hm.get("freebase-postprocessed"));
		//generateMetaPathsOfLengthDatabase(hm.get("relation"), hm.get("outfile"), hm.get("latest-category-graph-no-duplicates"), hm.get("table-name1"), Integer.parseInt(hm.get("length-meta-path")));
		
	}
	 
}
