package tests;

import java.io.FileNotFoundException;

import parser.Query;
import parser.QueryParser;

public class ParserTest {
	public static void main(String[] args){
		try {
			Query q1 = QueryParser.parse("queries/p4/query_1a.txt");
			System.out.println(q1);

			Query q2 = QueryParser.parse("queries/p4/query_1b.txt");
			System.out.println(q2);

			Query q3 = QueryParser.parse("queries/p4/query_2a.txt");
			System.out.println(q3);

			Query q4 = QueryParser.parse("queries/p4/query_2b.txt");
			System.out.println(q4);

			Query q5 = QueryParser.parse("queries/p4/query_2c.txt");
			System.out.println(q5);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
