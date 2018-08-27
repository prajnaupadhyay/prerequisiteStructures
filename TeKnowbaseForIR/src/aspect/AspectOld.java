package aspect;

import java.util.*;
import java.util.Map.Entry;
import java.io.*;

import preprocess.AdjList;
import preprocess.Edge;
import preprocess.ReadSubgraph;

import buildAndProcessTrees.BuildTree;
import buildAndProcessTrees.GetPropertyValues;

/** CURRENNTLY OF NO USE
 * a class to represent the aspect of a prerequisite concept, currently of no use
 * @author prajna
 *
 */
public class AspectOld
{
	/**
	 * for each input concept, this function generates a file consisting of the concept mentioned in its Wikipedia page and its Wikipedia category
	 * @throws Exception
	 */
	public static void generateCategories() throws Exception
	{
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		String file = hm.get("wikipedia-all-links-tsv");
		BufferedReader br1 = new BufferedReader(new FileReader(file));
		ReadSubgraph rr = new ReadSubgraph();
		AdjList a = rr.readFromFile(br1);
		System.out.println("read wikipedia links");
		ArrayList<String> inputConcepts = readInputSet(hm);
		BuildTree b = new BuildTree();
		//BuildTree b = new BuildTree();
		
		HashMap<String, String> redirections = b.handleRedirectionsOnly(hm.get("redirections"));
		HashMap<String, ArrayList<String>> categories = b.readWikiPageCategories("page-categories");
		
		//File f = new File("/mnt/dell/prajna/refdScores/aspect/");
		for(String a1:inputConcepts)
		{
			String redirectedName = redirections.get(a1);
			//bw.write(redirectedName+"\t");
			if(redirectedName==null) redirectedName = a1;
			HashMap<String, Integer> categoryCount = new HashMap<String, Integer>();
			HashMap<String, ArrayList<String>> catListPages = new HashMap<String, ArrayList<String>>();
			File f1 = new File("/mnt/dell/prajna/refdScores/aspect/"+a1);
			if(!f1.exists())
				f1.mkdir();
			BufferedWriter bw = new BufferedWriter(new FileWriter("/mnt/dell/prajna/refdScores/aspect/"+a1+"/categoryDistribution"));
			ArrayList<Edge> ee = a.getAdjList().get(redirectedName);
			for(Edge e:ee)
			{
				ArrayList<String> catlist = categories.get(e.getName());
				if(catlist!=null)
				{
					for(String c:catlist)
					{
						if(catListPages.get(c)==null)
						{
							ArrayList<String> list1 = new ArrayList<String>();
							list1.add(e.getName());
							catListPages.put(c, list1);
						}
						else
						{
							ArrayList<String> list1 = catListPages.get(c);
							list1.add(e.getName());
							catListPages.put(c, list1);
						}
						
						if(categoryCount.get(c)==null)
						{
							categoryCount.put(c, 1);
						}
						else
						{
							int count = categoryCount.get(c);
							categoryCount.put(c, count+1);
						}
					}
				}
			}
			List<Entry<String, Integer>> entries = new ArrayList<Entry<String, Integer>>(categoryCount.entrySet());
			
			 Comparator<Entry<String, Integer>> valueComparator = new Comparator<Entry<String, Integer>>() 
						{
							 public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2)
							 {
								 Integer v1 = e1.getValue();
								 Integer v2 = e2.getValue();
								 return v2.compareTo(v1);
							 }
						};
			Collections.sort(entries, valueComparator);
			for(Entry d:entries)
			{
				for(String s:catListPages.get(d.getKey()))
				{
					bw.write(d.getKey()+"\t"+s+"\t"+d.getValue()+"\n");
				}
				
			}
			bw.close();
		}
	}
	
	public static void pruneWikipediaFile() throws Exception
	{
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		String file = hm.get("wikipedia-all-links-tsv");
		BufferedReader br1 = new BufferedReader(new FileReader(file));
		ReadSubgraph rr = new ReadSubgraph();
		AdjList a = rr.readFromFile(br1);
		System.out.println("read wikipedia links");
		BuildTree b = new BuildTree();
		file = "/mnt/dell/prajna/sqlDumps/categoryAndSuperCategoriesLevel2InputSet29";
		BufferedReader br2 = new BufferedReader(new FileReader(file));
		String line;
		HashMap<String, HashMap<String, String>> hm1 = new HashMap<String, HashMap<String, String>>();
		while((line=br2.readLine())!=null)
		{
			StringTokenizer tok = new StringTokenizer(line,"\t");
			ArrayList<String> tokens = new ArrayList<String>();
			while(tok.hasMoreTokens())
			{
				tokens.add(tok.nextToken());
				//HashMap<String, String> hh = hm1.get(tok)
			}
			HashMap<String, String> hh = hm1.get(tokens.get(0));
			if(hh==null)
			{
				hh=new HashMap<String, String>();
				hh.put(tokens.get(1), "");
			}
			else
			{
				hh.put(tokens.get(1), "");
			}
			hm1.put(tokens.get(0), hh);
		}
		
		System.out.println("read the categories");
		//ReadSubgraph rr = new ReadSubgraph();
		//AdjList a2 = rr.readFromFile(br2);
		HashMap<String, String> redirections = b.handleRedirectionsOnly(hm.get("redirections"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("/mnt/dell/prajna/neo4j/input/allLinksPrunedByCategory.tsv"));
		for(String a1:a.getAdjList().keySet())
		{
			String redirectedName = redirections.get(a1);
			if(redirectedName==null) redirectedName = a1;
			if(hm1.get(redirectedName)==null) 
			{
				ArrayList<Edge> ee = a.getAdjList().get(redirectedName);
				if(ee==null) continue;
				else
				{
					for(Edge e:ee)
					{
						bw.write(redirectedName+"\t"+e.getName()+"\n");
					}
				}
				continue;
			}
			else
			{
				HashMap<String, String> hh = hm1.get(redirectedName);
				
				if(hh.get("software")!=null || hh.get("programming_languages")!=null || hh.get("tools")!=null || hh.get("products_by_company")!=null)
				{
					continue;
				}
				else
				{
					ArrayList<Edge> ee = a.getAdjList().get(redirectedName);
					if(ee==null) continue;
					else
					{
						for(Edge e:ee)
						{
							HashMap<String, String> hh1 = hm1.get(e.getName());
							if(hh1==null)
							{
								bw.write(redirectedName+"\t"+e.getName()+"\n");
								continue;
							}
							else if(hh1.get("software")!=null || hh1.get("programming_languages")!=null || hh1.get("tools")!=null || hh1.get("products_by_company")!=null)
							{
								continue;
							}
							else
							{
								bw.write(redirectedName+"\t"+e.getName());
							}
						}
					}
				}
			}
		}
		bw.close();
	}
	/**
	 * function that has all other functions written in a sequence
	 */
	
	public static void sequence() throws Exception
	{
		generateCategories();
		generateAllCategoryList();
		//generateSmallerSet();
	}
	
	/**
	 * a function to read input concepts
	 * @param hm
	 * @return
	 * @throws Exception
	 */
	
	public static ArrayList<String> readInputSet(HashMap<String, String> hm) throws Exception
	{
		String line;
		BufferedReader br = new BufferedReader(new FileReader(hm.get("input-concepts")));
		ArrayList<String> inputConcepts = new ArrayList<String>();
		while((line=br.readLine())!=null)
		{
			inputConcepts.add(line.trim().replace(" ", "_").toLowerCase());
		}
		return inputConcepts;
	}
	
	/**
	 * produces a new file "categoryAndSuperCategoriesLevel2" where each page's categories are its immediate categories and their categories and super categories
	 * @throws Exception
	 */
	
	public static void generateAllCategoryList() throws Exception
	{
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		BuildTree b = new BuildTree();
		ReadSubgraph rr = new ReadSubgraph();
		String file = hm.get("wikipedia-all-links-tsv");
		BufferedReader br1 = new BufferedReader(new FileReader(file));
		//ReadSubgraph rr = new ReadSubgraph();
		AdjList a1 = rr.readFromFile(br1);
		System.out.println("read wikipedia links");
		AdjList catSubcats = rr.readFromFile(new BufferedReader(new FileReader("/mnt/dell/prajna/sqlDumps/categoryAndTheirSubacteories")));
		HashMap<String, ArrayList<String>> incidentList = catSubcats.getIncidentList();
		BufferedWriter bw = new BufferedWriter(new FileWriter("/mnt/dell/prajna/sqlDumps/categoryAndSuperCategoriesLevel2InputSet29"));
		HashMap<String, ArrayList<String>> categories = b.readWikiPageCategories("page-categories");
		ArrayList<String> inputConcepts = readInputSet(hm);
		HashMap<String, String> moreConcepts = new HashMap<String, String>();
		HashMap<String, String> redirections = b.handleRedirectionsOnly(hm.get("redirections"));
		for(String a:inputConcepts)
		{
			moreConcepts.put(a, "");
			String redirectedName = redirections.get(a);
			if(redirectedName==null) redirectedName=a;
			ArrayList<Edge> ee = a1.getAdjList().get(redirectedName);
			for(Edge e:ee)
			{
				moreConcepts.put(e.getName(),"");
			}
		}
		for(String a:moreConcepts.keySet())
		{
			String redirectedName = redirections.get(a);
			if(redirectedName==null) redirectedName=a;
			ArrayList<String> catList = categories.get(redirectedName);
			ArrayList<String> catList1 = new ArrayList<String>();
			if(catList==null) continue;
			for(String b1:catList)
			{
				catList1.add(b1);
				//ArrayList<Edge> supCategories = catSubcats.getAdjList().get(b1);
				ArrayList<String> supCategories = incidentList.get(b1);
				if(supCategories!=null)
				{
					for(String e:supCategories)
					{
						catList1.add(e);
						ArrayList<String> supsupCategories = incidentList.get(e);
						if(supsupCategories!=null)
						{
							for(String ss:supsupCategories)
							{
								catList1.add(ss);
							}
						}
					}
				}
			}
			catList1.addAll(catList);
			for(String c:catList1)
			{
				bw.write(a+"\t"+c+"\n");
			}
		}
		bw.close();
	}
	
	/**
	 * generates a file that has the category information (up to level 2) for only the concepts mentioned in the input set and their Wikipedia neighbours
	 * @throws Exception
	 */
	
	public static void generateSmallerSet() throws Exception
	{
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		String file = hm.get("wikipedia-all-links-tsv");
		
		BuildTree b = new BuildTree();
		BufferedReader br1 = new BufferedReader(new FileReader(file));
		ReadSubgraph rr = new ReadSubgraph();
		AdjList a = rr.readFromFile(br1);
		System.out.println("read all wikipedia links");
		AdjList catSubcats = rr.readFromFile(new BufferedReader(new FileReader("/mnt/dell/prajna/sqlDumps/categoryAndSuperCategoriesLevel2")));
		System.out.println("read category and supercategories level 2");
		ArrayList<String> inputConcepts = readInputSet(hm);
		System.out.println("read input set");
		HashMap<String, String> redirections = b.handleRedirectionsOnly(hm.get("redirections"));
		System.out.println("read redirections");
		HashMap<String, HashMap<String, String>> newAdjList = new HashMap<String, HashMap<String, String>>();
		for(String a1:inputConcepts)
		{
			System.out.println(a1);
			String redirectedName = redirections.get(a1);
			if(redirectedName==null) redirectedName = a1;
			ArrayList<Edge> list = a.getAdjList().get(redirectedName);
			for(Edge l:list)
			{
				String name = l.getName();
				ArrayList<Edge> catList = catSubcats.getAdjList().get(name);
				if(catList==null) continue;
				if(newAdjList.get(name)==null)
				{
					HashMap<String, String> hh = new HashMap<String, String>();
					for(Edge ee:catList)
					{
						hh.put(ee.getName(), "");
					}
					newAdjList.put(name, hh);
				}
				else
				{
					HashMap<String, String> hh = newAdjList.get(name);
					for(Edge ee:catList)
					{
						hh.put(ee.getName(), "");
					}
					newAdjList.put(name, hh);
				}
			}
			ArrayList<Edge> catList = catSubcats.getAdjList().get(redirectedName);
			if(catList==null) continue;
			if(newAdjList.get(redirectedName)==null)
			{
				HashMap<String, String> hh = new HashMap<String, String>();
				for(Edge ee:catList)
				{
					hh.put(ee.getName(), "");
				}
				newAdjList.put(redirectedName, hh);
			}
			else
			{
				HashMap<String, String> hh = newAdjList.get(redirectedName);
				for(Edge ee:catList)
				{
					hh.put(ee.getName(), "");
				}
				newAdjList.put(redirectedName, hh);
			}
			
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter("/mnt/dell/prajna/sqlDumps/categorySuperCategoriesLevel2InputSet29"));
		for(String a1:newAdjList.keySet())
		{
			for(String a2:newAdjList.get(a1).keySet())
			{
				bw.write(a1+"\t"+a2+"\n");
			}
		}
		bw.close();
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		pruneWikipediaFile();
		//generateAllCategoryList();
		//generateAllCategoryList();
	//	generateSmallerSet();
		/*String file = hm.get("wikipedia-all-links-tsv");
		BufferedReader br1 = new BufferedReader(new FileReader(file));
		ReadSubgraph rr = new ReadSubgraph();
		AdjList a = rr.readFromFile(br1);
		System.out.println("read wikipedia links");
		System.out.println(a.getAdjList().size());*/
		//generateCategories();
		
		
		//ArrayList<String> inputSet = readInputSet(hm);
	}

}
