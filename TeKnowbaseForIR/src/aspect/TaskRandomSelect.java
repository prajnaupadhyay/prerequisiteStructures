package aspect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

import com.google.common.graph.*;

/**
 * class to parallelize the random walks. A single walk of 100 steps is computed by each thread. 
 * @author pearl
 *
 */
public class TaskRandomSelect implements Runnable
{
	AdjListCompact a; // This graph is the adjacency list where there is an edge between nodes 'a' and 'b' there exists a meta-path (given as input) from a to b.  At each step, it randomly selects a neighbor for a node. 
	int walklength;
	String aa;
	//ArrayList<ArrayList<Integer>> aa;
	//HashMap<Integer, ArrayList<ArrayList<Integer>>> randomwalk; // stores the walks that have been generated till now
	int node;
	int orgnode;
	int startindex;
	HashMap<Integer, String> randomwalk;
	
	public TaskRandomSelect(AdjListCompact a, int walklength, String aa, int node, int orgnode, int startindex, HashMap<Integer, String> randomwalk)
	{
		this.a = a;
		this.walklength=walklength;
		this.aa = aa;
		this.node = node;
		this.startindex = startindex;
		this.orgnode = orgnode;
		this.randomwalk = randomwalk;
		
	}
	@Override
	public void run() 
	{
		Random r = new Random();
		// TODO Auto-generated method stub
		String randomwalk1 = ""+node;
		//randomwalk1.add(node);
		ArrayList<Integer> al = a.pathGraphHashmap.get(node);
		for(int j=startindex;j<walklength-1;j=j+2)
		{
			int i = r.nextInt(al.size());
			//randomwalk1.add(al.get(i));
			randomwalk1 = randomwalk1 +" "+ al.get(i);
			ArrayList<Integer> al1 = a.pathGraphHashMapReverse.get(al.get(i));
			if(al1!=null)
			{
				i = r.nextInt(al1.size());
				//randomwalk1.add(al1.get(i));
				randomwalk1 = randomwalk1 + " " + al1.get(i);
			}
		}
		
		//System.out.println(randomwalk1);
		
		
		{
			
			//aa.add(randomwalk1);
			
			randomwalk.put(node, randomwalk.get(node)+"\n"+randomwalk1);
			
		}
		
	}
	

}
