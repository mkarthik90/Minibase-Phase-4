package tests;

import iterator.IEJoinInMemoryP4;
import parser.Query;
import parser.QueryParser;

public class IEJoinInMemoryP4Test {
	public static void main(String[] args){
		try {
			//DBBuilderP4.build();
			long startTime = System.currentTimeMillis();
			DBBuilderP4.buildNumTuples(1000);
			//DBBuilder.build();
			Query query = QueryParser.parse("queries/phase4_query.txt");
			IEJoinInMemoryP4 test = new IEJoinInMemoryP4(query, false);
			System.out.println("Number of tuples: " + test.getNumTuples());
			long endTime = System.currentTimeMillis();
			long difference = startTime - endTime;
			System.out.println("Time taken"+difference);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
