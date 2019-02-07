package aspect;
import java.util.ArrayList;
import java.util.HashSet;

import preprocess.Edge;

import LMForTeKnowbase.LanguageModelEntity;


public class TaskNormalize implements Runnable
{
	ArrayList<LanguageModelEntity> lm;
	int i;
	HashSet<Edge> h;
	public TaskNormalize(ArrayList<LanguageModelEntity> lm, int i, HashSet<Edge> h)
	{
		this.lm=lm;
		this.i=i;
		this.h = h;
	}
	public void run() 
	{
		LanguageModelEntity l = lm.get(i);
		for(int j=i+1;j<lm.size();j++)
		{
			LanguageModelEntity l1 = lm.get(j);
			double cosine_sim = l.cosineSimilarity(l1);
			h.add(new Edge((float)cosine_sim, l.getName(), l1.getName()));
		}
	}
}
