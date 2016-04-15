package tests;

import heap.Tuple;
import iterator.CondExpr;

public class Phase4Part2 {

	public static int join_tables(CondExpr cond[])
	{
		int estimates[] = new int[cond.length];
		Tuple product[] = null;
		boolean first = true;
		//get estimates
		for(int i = 0; i < cond.length; i++)
		{
			estimates[i] = 0;//evaluate_condition(cond[i])
		}
		
		
		//for each join
		for(int i = 0; i < cond.length; i++)
		{
			int minindex = 0;
			int minamount = 2147483647;
			//execute joins
			for(int j = 0; j < cond.length; j++)
			{
				if(estimates[j] != -1 && minamount > estimates[j])
				{
					minindex = j;
					minamount = estimates[j];
				}
				estimates[minindex] = -1;
				if(first)
				{
					product = null; //join the two
				}
				else
				{
					product = null; //join the results with the new
				}
			}
		}	
		return product.length;
	}
}
