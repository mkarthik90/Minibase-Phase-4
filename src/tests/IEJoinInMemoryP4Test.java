package tests;

import iterator.IEJoinInMemoryP4;
import parser.Query;
import parser.QueryParser;

public class IEJoinInMemoryP4Test {
	public static void main(String[] args){
		try {
			DBBuilderP4.build();
			//DBBuilder.build();
			Query query = QueryParser.parse("queries/p4/query_3a.txt");
			IEJoinInMemoryP4 test = new IEJoinInMemoryP4(query, false);
		
			System.out.println(test);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
