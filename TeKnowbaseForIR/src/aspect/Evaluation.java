package aspect;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.io.*;

import buildAndProcessTrees.GetPropertyValues;

/**
 * class with functions related to evaluation portal
 * @author pearl
 *
 */

public class Evaluation 
{
	/**
	 * given a query, reads all papers retrieved across all heuristics and generates a hashmap of unique paper names to their abstract names
	 * @param q
	 * @param heuristiclist
	 * @param folder
	 * @return
	 * @throws Exception
	 */
	public static HashMap<String, String> readPapersFromFolders(String q, ArrayList<String> heuristiclist, String folder) throws Exception
	{
		HashMap<String, String> paperlist = new HashMap<String, String>();
		for(String h:heuristiclist)
		{
			File f = new File(folder+"/"+h);
			for(File f1:f.listFiles())
			{
				if(f1.isDirectory())
				{
					File ff = new File(f1.getAbsolutePath()+"/"+q);
					if(ff.exists())
					{
						BufferedReader br = new BufferedReader(new FileReader(f1.getAbsolutePath()+"/"+q));
						String line;
						while((line=br.readLine())!=null)
						{
							StringTokenizer tok = new StringTokenizer(line,"\t");
							ArrayList<String> tokens = new ArrayList<String>();
							while(tok.hasMoreTokens())
							{
								tokens.add(tok.nextToken());
							}
							paperlist.put(tokens.get(1), tokens.get(2));
						}
					}
					
				}
			}
		}
		return paperlist;
	}
	
	/**
	 * reads across all heuristics and writes down all unique papers retrieved for a query into a file
	 * @param queries
	 * @param folder
	 * @param heuristics
	 * @throws Exception
	 */
	public static void getPapersForAllQueries(String queries, String folder, String heuristics) throws Exception
	{
		BufferedReader br1 = new BufferedReader(new FileReader(heuristics));
		String line1;
		ArrayList<String> heuristiclist = new ArrayList<String>();
		while((line1=br1.readLine())!=null)
		{
			heuristiclist.add(line1);
		}
		BufferedReader br = new BufferedReader(new FileReader(queries));
		String line;
		ArrayList<String> querylist = new ArrayList<String>();
		while((line=br.readLine())!=null)
		{
			querylist.add(line);
		}
		for(String q:querylist)
		{
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(folder+"/all_papers_together_top_5/"+q));
			HashMap<String, String> paperlist = readTop5PapersFromFolders(q, heuristiclist, folder);
			for(String s:paperlist.keySet())
			{
				bw.write(s+"\t"+paperlist.get(s)+"\n");
			}
			bw.close();
		}
		
	}
	
	/**
	 * same as readPapersFromFolders, but creates the list of unique papers by reading top 5 papers only
	 * @param queries
	 * @param folder
	 * @param heuristics
	 * @throws Exception
	 */
	
	public static HashMap<String, String> readTop5PapersFromFolders(String q, ArrayList<String> heuristiclist, String folder) throws Exception
	{
		HashMap<String, String> paperlist = new HashMap<String, String>();
		for(String h:heuristiclist)
		{
			File f = new File(folder+"/"+h);
			for(File f1:f.listFiles())
			{
				if(f1.isDirectory())
				{
					File ff = new File(f1.getAbsolutePath()+"/"+q);
					if(ff.exists())
					{
						BufferedReader br = new BufferedReader(new FileReader(f1.getAbsolutePath()+"/"+q));
						String line;
						int count=0;
						while((line=br.readLine())!=null)
						{
							StringTokenizer tok = new StringTokenizer(line,"\t");
							ArrayList<String> tokens = new ArrayList<String>();
							while(tok.hasMoreTokens())
							{
								tokens.add(tok.nextToken());
							}
							count++;
							if((count>=1 && count<=5) || (count>=11 && count<=15) || (count>=21 && count<=25) || (count>=31 && count<=35) && (count>=41 && count<=45))
							{
								paperlist.put(tokens.get(1), tokens.get(2));
							}
							
						}
					}
					
				}
			}
		}
		return paperlist;
	}
	
	public static void connect(String table_name, String uname, String pwd, String dbname) throws Exception
	{
		System.out.println(table_name);
		//Class.forName("org.postgresql.Driver");
		Class.forName("com.mysql.jdbc.Driver");  
		System.out.println("Mysql JDBC Driver Registered!");
		Connection connection = null;
		connection = DriverManager.getConnection(
				"jdbc:mysql://127.0.0.1:3307/"+dbname, uname,
				pwd);
		
		if (connection != null) 
		{
			System.out.println("DBAdjList has been initialized");
			
		}
		else
		{
			System.out.println("Could not connect with username and password");
			System.exit(0);
		}
	}
	
	/**
	 * master function to connect to db abd evaluate results
	 * @param queries
	 * @param folder
	 * @param heuristics
	 * @param table_name
	 * @param uname
	 * @param pwd
	 * @param dbname
	 * @throws Exception
	 */
	
	public static void readResults(String queries, String folder, String heuristics, String table_name, String uname, String pwd, String dbname) throws Exception
	{
		System.out.println(table_name);
		//Class.forName("org.postgresql.Driver");
		Class.forName("com.mysql.jdbc.Driver");  
		System.out.println("Mysql JDBC Driver Registered!");
		Connection connection = null;
		connection = DriverManager.getConnection(
				"jdbc:mysql://127.0.0.1:3307/"+dbname, uname,
				pwd);
		
		if (connection != null) 
		{
			System.out.println("DBAdjList has been initialized");
			
		}
		else
		{
			System.out.println("Could not connect with username and password");
			System.exit(0);
		}
		BufferedReader br1 = new BufferedReader(new FileReader(heuristics));
		String line1;
		ArrayList<String> heuristiclist = new ArrayList<String>();
		while((line1=br1.readLine())!=null)
		{
			heuristiclist.add(line1);
		}
		BufferedReader br = new BufferedReader(new FileReader(queries));
		String line;
		ArrayList<String> querylist = new ArrayList<String>();
		while((line=br.readLine())!=null)
		{
			querylist.add(line);
		}
		for(String q:querylist)
		{
			
			//BufferedWriter bw = new BufferedWriter(new FileWriter(folder+"/all_papers_together_top_5/"+q));
			evaluate(q, heuristiclist, folder, connection, table_name);
			
		}
		
		
	}
	
	/**
	 * writes down the results to a file
	 * @param q
	 * @param heuristiclist
	 * @param folder
	 * @param connection
	 * @param table_name
	 * @param uname
	 * @param pwd
	 * @param dbname
	 * @throws Exception
	 */
	
	public static void evaluate(String q, ArrayList<String> heuristiclist, String folder, Connection connection, String table_name) throws Exception
	{
		BufferedWriter bw1 = new BufferedWriter(new FileWriter(folder+"/results/"+q));
		for(String h:heuristiclist)
		{
			File f = new File(folder+"/"+h);
			for(File f1:f.listFiles())
			{
				if(f1.isDirectory())
				{
					
					ArrayList<Integer> i = query(q, f1, folder, connection, table_name);
					double sum=0;
					for(int ii:i)
					{
						sum = sum + ii;
					}
					if(f1.getName().equals("algorithm"))
						bw1.write(h+"\t"+f1.getName()+"\t"+sum/(i.size()*3)+"\n");
					else if(f1.getName().equals("application"))
						bw1.write(h+"\t"+f1.getName()+"\t"+sum/(i.size()*2)+"\n");
					else if(f1.getName().equals("implementation"))
						bw1.write(h+"\t"+f1.getName()+"\t"+sum/(i.size()*2)+"\n");
					
				}
			}
		}
		bw1.close();
	}
	
	/**
	 * This function returns the array of evals for each query, paper and aspect triple
	 * @param q: query
	 * @param aspect: aspect name
	 * @param heuristiclist: list of heuristic lists
	 * @param folder: root folder
	 * @param connection: mysql connection
	 * @param table_name: table where evals are stored
	 * @param uname: user name
	 * @param pwd: password
	 * @param dbname: name of db
	 * @return
	 * @throws Exception
	 */
	
	public static ArrayList<Integer> query(String q, File aspect, String folder, Connection connection, String table_name) throws Exception
	{
		File ff = new File(aspect.getAbsolutePath()+"/"+q);
		ArrayList<Integer> i = new ArrayList<Integer>();
		if(ff.exists())
		{
			BufferedReader br = new BufferedReader(new FileReader(aspect.getAbsolutePath()+"/"+q));
			String line;
			int count=0;
			
			while((line=br.readLine())!=null)
			{
				StringTokenizer tok = new StringTokenizer(line,"\t");
				ArrayList<String> tokens = new ArrayList<String>();
				while(tok.hasMoreTokens())
				{
					tokens.add(tok.nextToken());
				}
				count++;
				if((count>=1 && count<=5) || (count>=11 && count<=15) || (count>=21 && count<=25) || (count>=31 && count<=35) && (count>=41 && count<=45))
				{
					//paperlist.put(tokens.get(1), tokens.get(2));
					String a = tokens.get(1);
					String b = tokens.get(2);
					String sql1 = "select eval from "+table_name+" where query=? and paper=? and aspect=?";
					PreparedStatement statement1 = connection.prepareStatement(sql1);
					statement1.setString(1, q);
					statement1.setString(2, a);
					statement1.setString(3, aspect.getName());
					ResultSet rs1 = statement1.executeQuery();
					int val=0;
					int count1=0;
					while(rs1.next())
					{
						count1++;
						val=rs1.getInt(1);
						i.add(val);
						break;
					}
					if(count1==0 && q.equals("minimum_spanning_tree")) System.out.println(a);
				}
				
			}
		}
		return i;
	}
	
	
	public static void main(String args[]) throws Exception
	{
		GetPropertyValues properties = new GetPropertyValues();
		HashMap<String, String> hm = properties.getPropValues();
		System.out.println("hiiiii");
		//connect("evaluation", "root", "admin", "tkbforir_evaluation");
		readResults(hm.get("query-file"), hm.get("parent-folder"), hm.get("heuristics"), "evaluation", "root", "admin", "tkbforir_evaluation");
	}
	
}
