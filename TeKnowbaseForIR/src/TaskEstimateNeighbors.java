import java.util.ArrayList;
import java.util.HashMap;

import aspect.AdjListCompact;
import aspect.Aspect;

/**
 * class to parallelize the estimation of neighbors at each step
 * @author pearl
 *
 */

public class TaskEstimateNeighbors implements Runnable
{
	int[][] rws;
	AdjListCompact a;
	int walklength;
	int numwalks;
	int node;
	Aspect as;
	
	public TaskEstimateNeighbors(int[][] rws, AdjListCompact a, int walklength, int numwalks, int node, Aspect as)
	{
		this.rws = rws;
		this.a = a;
		this.walklength = walklength;
		this.numwalks = numwalks;
		this.node = node;
		this.as = as;
	}
	
	@Override
	public void run() 
	{
		// TODO Auto-generated method stub
		ArrayList<Integer> ll = a.pathGraphHashmap.get(node);
		//ArrayList<Integer> ll = new ArrayList<Integer>(s);
	//	if(ll==null) continue;
	//	if(ll.size()==0) continue;
	//	System.out.println("node: "+n+", neighbor size: "+ll.size());
		
		//	System.out.println("neighbour: "+ll);
		
		//System.out.println("\n");
		
		for(int i=0;i<1000;i++)
		{
			rws[i][0]=node;
		}
		for(int j=1;j<100;j=j+1)
		{
			HashMap<Integer, ArrayList<Integer>> source = new HashMap<Integer, ArrayList<Integer>>();
			for(int i=0;i<1000;i++)
			{
				if(source.get(rws[i][j-1])==null)
				{
					ArrayList<Integer> indexlist = new ArrayList<Integer>();
					indexlist.add(i);
					source.put(rws[i][j-1], indexlist);
				}
				else
				{
					ArrayList<Integer> indexlist = source.get(rws[i][j-1]);
					indexlist.add(i);
					source.put(rws[i][j-1], indexlist);
				}
			}
			//System.out.println("unique nodes at step "+j+": "+source.keySet());
			if(j % 2==0)
			{
				as.estimateNeighborsAtIndex1(j,source,a,rws,a.pathGraphHashMapReverse);
				//estimateNeighborsAtIndex(j,n,a,rws,a.pathGraphHashMapReverse);
			}
			else if(j%2==1)
			{
				as.estimateNeighborsAtIndex1(j,source,a,rws,a.pathGraphHashmap);
				//estimateNeighborsAtIndex(j,n,a,rws,a.pathGraphHashMapReverse);
			}
		}
		
	}

}
