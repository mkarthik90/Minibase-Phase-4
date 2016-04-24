package tests;

import iterator.IEJoinInMemoryP4;
import parser.Query;
import parser.QueryParser;

public class IEJoinInMemoryP4Test {
	public static void main(String[] args){
		try {
			//DBBuilderP4.build();
			DBBuilderP4.buildNumTuples(1000);
			//DBBuilder.build();
			Query query = QueryParser.parse("queries/phase4_query.txt");
			IEJoinInMemoryP4 test = new IEJoinInMemoryP4(query, false);
			
			System.out.println("Number of tuples: " + test.getNumTuples());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
