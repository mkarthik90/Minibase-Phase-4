package tests;

import iterator.CondExpr;


public class P4P2test {

	public static void main(String arg[])
	{
		int size = 0;
		CondExpr cond[] = null;
		Histogram hist[] = null;
		hist = DBBuilderP4.build_sample();
		
		size = Phase4Part2.join_tables(cond);
		
		System.out.println("Multipredicate count: " + size);
	}
}
