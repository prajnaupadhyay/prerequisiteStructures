package aspect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

public class TaskRandomSelect implements Runnable
{
	AdjListCompact a;
	int walklength;
	HashMap<Integer, ArrayList<ArrayList<Integer>>> randomwalk;
	int node;
	
	public TaskRandomSelect(AdjListCompact a, int walklength, HashMap<Integer, ArrayList<ArrayList<Integer>>> randomwalk, int node)
	{
		this.a = a;
		this.walklength=walklength;
		this.randomwalk = randomwalk;
		this.node = node;
	}
	@Override
	public void run() 
	{
		Random r = new Random();
		// TODO Auto-generated method stub
		ArrayList<Integer> randomwalk1 = new ArrayList<Integer>();
		randomwalk1.add(node);
		Set<Integer> ss = a.pathGraph.successors(node);
		for(int j=1;j<walklength;j=j+2)
		{
			ArrayList<Integer> al = new ArrayList<Integer>(ss);
			int i = r.nextInt(al.size());
			randomwalk1.add(al.get(i));
			ArrayList<Integer> al1 = new ArrayList<Integer>(a.pathGraph_inverse.successors(al.get(i)));
			if(al1!=null)
			{
				i = r.nextInt(al1.size());
				randomwalk1.add(al1.get(i));
			}
		}
		
		if(randomwalk.get(node)==null)
		{
			ArrayList<ArrayList<Integer>> aa = new ArrayList<ArrayList<Integer>>();
			aa.add(randomwalk1);
			randomwalk.put(node, aa);
		}
		else
		{
			ArrayList<ArrayList<Integer>> aa = randomwalk.get(node);
			aa.add(randomwalk1);
			randomwalk.put(node, aa);
		}
		
	}
	

}
