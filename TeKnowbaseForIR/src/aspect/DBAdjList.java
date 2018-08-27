package aspect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.Map.Entry;
import java.io.*;

import preprocess.AdjList;
import preprocess.Edge;
import preprocess.ReadSubgraph;

public class DBAdjList {

	/**
	 * @param A class to represent the graph structure stored in a database
	 */
	Connection con;
	String table_name;
	String dbname;
	
	
	public DBAdjList(String table_name, String uname, String pwd, String dbname) throws Exception
	{
		System.out.println(table_name);
		this.table_name = table_name;
		Class.forName("org.postgresql.Driver");
		System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		connection = DriverManager.getConnection(
				"jdbc:postgresql://127.0.0.1:5432/"+dbname, uname,
				pwd);
		
		if (connection != null) 
		{
			System.out.println("DBAdjList has been initialized");
			this.con = connection;
		}
		else
		{
			System.out.println("Could not connect with username and password");
			System.exit(0);
		}
		
	}
	
	/**
	 * this function queries the database and retrieves the neighbours of an entity
	 * @param name: entity for which we will retrieve the neighbours
	 * @return
	 * @throws Exception
	 */
	
	public ArrayList<Edge> retrieveNeighbours(String name) throws Exception
	{
		String sql = "select * from "+table_name+" where arg1 = ? and md5(arg1) = md5(?)";
		System.out.println(sql);
		PreparedStatement  statement = this.con.prepareStatement(sql);
		statement.setString(1, name);
		statement.setString(2, name);
		ResultSet rs1 = statement.executeQuery();
		//aa1=format(aa1);
		ArrayList<Edge> ee = new ArrayList<Edge>();
		while(rs1.next())
		{
			Edge e = new Edge(0,rs1.getString(3), rs1.getString(2));
			ee.add(e);
		}
		return ee;
	}
	
	/**
	 * this module is exactly same as the module that retrieves k-hop paths for AdjList class. 
	 * @param inputConcept
	 * @param target
	 * @param k
	 * @param bw
	 * @throws Exception
	 */
	
	public void allKHopPaths(String inputConcept, String target, int k, BufferedWriter bw) throws Exception
	{
		
		LinkedList<Edge> queue = new LinkedList<Edge>();
		queue.add(new Edge(0,inputConcept,""));
		HashMap<String, Edge> pred = new HashMap<String, Edge>();
		int count=0;
		while(queue.size()>0 && count<=k)
		{
			Edge a=queue.removeFirst();
			ArrayList<Edge> e1 = retrieveNeighbours(a.getName());
			//System.out.println("done");
			for(Edge neigh: e1)
			{
				queue.add(neigh);
				
				pred.put(neigh.getName(), a);
				if(count==k && (neigh.getName().equals(target) || neigh.getName().equals(target.replace("category:", ""))))
				{
					String p = neigh.getName();
					bw.write(p+"\t"+neigh.getRelationaName()+"\t");
					
					int cc =1;
					while(pred.get(p)!=null && cc<k)
					{
						Edge eee = pred.get(p);
						p = eee.getName();
						bw.write(p+"\t"+eee.getRelationaName()+"\t");
						cc++;
					}
					bw.write(inputConcept+"\n");
				}
			}
			count++;
			
		}
		
		
		
	}
	
	

}
