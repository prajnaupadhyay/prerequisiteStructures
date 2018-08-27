package aspect;

import java.io.BufferedWriter;
import java.util.*;

public class TaskRandomWalkOld implements Runnable
{
	String node;
	ArrayList<String> path;
	AdjListCompactOld a;
	int walklength;
	HashMap<String, String> randomwalk;
	HashMap<String, HashMap<String, Set<String>>> h;
	
	public TaskRandomWalkOld(String node, ArrayList<String> path, AdjListCompactOld a, int walklength, HashMap<String, HashMap<String, Set<String>>> h, HashMap<String, String> randomwalk)
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
			String randomwalk1 = node;
			
			for(int j=0;j<walklength;j++)
			{
				String node1 = a.randomWalk(path, 0, node, h);
				if(!node1.equals("")) 
				{
					randomwalk1 = randomwalk1 + " "+node1;
					count++;
				}
				else 
				{
					//randomwalk1 = randomwalk1+"_not going further";
					break;
				}
				String node2 = a.randomWalk(a.inverse(path),0,node1, h);
				if(!node2.equals("")) 
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
