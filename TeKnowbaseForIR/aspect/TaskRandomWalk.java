package aspect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class TaskRandomWalk implements Runnable
{
	int node;
	ArrayList<Integer> path;
	AdjListCompact a;
	int walklength;
	HashMap<Integer, String> randomwalk;
	HashMap<Long, Set<Integer>> h;
	
	public TaskRandomWalk(int node, ArrayList<Integer> path, AdjListCompact a, int walklength, HashMap<Long, Set<Integer>> h, HashMap<Integer, String> randomwalk)
	{
		this.node = node;
		this.path = path;
		this.a = a;
		this.walklength = walklength;
		this.h=h;
		this.randomwalk = randomwalk;
	}
	public void run()
	{
			int count=0;
			
			String randomwalk1 = "";
			for(int j=0;j<walklength;j++)
			{
				int node1 = a.randomWalk(path, 0, node, h);
				
					randomwalk1 = randomwalk1 + " "+ node1;
					count++;
				
				int node2 = a.randomWalk(a.inverse(path),0,node1, h);
				if(node2!=-1)
				{
					randomwalk1 = randomwalk1 + " "+ node2;
					count++;
				}
			}
			//System.out.println("here");
			
			{
				randomwalk1 = randomwalk1 + "\n";
			}
			if(randomwalk.get(node)==null)
			{
				randomwalk.put(node, randomwalk1);
			}
			else
			{
				randomwalk.put(node, randomwalk.get(node)+randomwalk1);
			}
				//System.out.println(randomwalk);
		
	}
}
