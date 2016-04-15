package tests;

import java.io.FileNotFoundException;

import parser.Query;
import parser.QueryParser;

public class ParserTest {
	public static void main(String[] args){
		try {
			Query q1 = QueryParser.parse("queries/p4/query_1a.txt");
			System.out.println(q1);
			System.out.println("============");

			Query q2 = QueryParser.parse("queries/p4/query_1b.txt");
			System.out.println(q2);
			System.out.println("============");

			Query q3 = QueryParser.parse("queries/p4/query_2a.txt");
			System.out.println(q3);
			System.out.println("============");

			Query q4 = QueryParser.parse("queries/p4/query_2b.txt");
			System.out.println(q4);
			System.out.println("============");

			Query q5 = QueryParser.parse("queries/p4/query_2c.txt");
			System.out.println(q5);
			System.out.println("============");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
