package aspect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

/**
 * A task that does all the randomwalks for each node.
 * @author pearl
 *
 */
public class TaskRandomWalkAlternate implements Runnable
{
	int node;
	AdjListCompact a;
	int walklength;
	int numwalks;
	HashMap<Integer, int[][]> randomwalks;
	
	public TaskRandomWalkAlternate(int node, AdjListCompact a, int walklength, int numwalks, HashMap<Integer, int[][]> randomwalks)
	{
		this.node = node;
		this.a = a;
		this.walklength = walklength;
		this.numwalks = numwalks;
		this.randomwalks = randomwalks;
	}
	public void run() 
	{
		Random r = new Random();
		// TODO Auto-generated method stub
		int[][] randomwalk1 = new int[numwalks][walklength+1];
		Set<Integer> ss = a.pathGraph.successors(node);
		ArrayList<Integer> al = new ArrayList<Integer>(ss);
		for(int i=0;i<numwalks;i++)
		{
			randomwalk1[i][0]=node;
			for(int j=1;j<=walklength;j=j+2)
			{
				int ii = r.nextInt(al.size());
				randomwalk1[i][j]=al.get(ii);
				ArrayList<Integer> al1 = new ArrayList<Integer>(a.pathGraph_inverse.successors(al.get(ii)));
				if(al1!=null)
				{
					ii = r.nextInt(al1.size());
					randomwalk1[i][j+1]=al1.get(ii);
				}
			}
		}
		
		randomwalks.put(node, randomwalk1);
		
	}

}
