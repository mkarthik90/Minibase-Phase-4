package tests;

import global.AttrOperator;

import java.io.File;
import java.util.Scanner;

public class HistogramTest {
	public static void main(String args[])
	{
		//File Name goes here---------------------------------------------------------
		String fn = "queries/p4/histquerry5.txt";
		//change input size-----------------------------------------------------------
		Histogram[] hist = DBBuilderP4.build_sorted();
		//----------------------------------------------------------------------------
		
		//create file
		File file = new File(fn);
	
		if (!file.exists()) {
			System.out.println("File at '" + fn + "' not found.");
		}
		
		//variables for parsing
		String line;
		int r1c1 = -1, r1c2 = -1, r2c1 = -1, r2c2 = -1, op1 = -1, op2 = -1;
		int table1 = 0, table2 = 0, table3 = 0, table4 = 0;
		Scanner scan;
		try{
			//create the scanner
			scan = new Scanner(file);
			//skip the first two lines
			line = scan.nextLine();
			line = scan.nextLine();
			//read the first line with content
			line = scan.nextLine();
			//split according to spaces
			String parts[] = line.trim().split(" ");
			String subparts[];
			//the first table is the number contained in the second character
			table1 = Integer.parseInt(parts[0].substring(1, 2));
			//done to get to the column number
			subparts = parts[0].trim().split("_");
			r1c1 = Integer.parseInt(subparts[1]);
			table2 = Integer.parseInt(parts[2].substring(1, 2));
			subparts = parts[2].trim().split("_");
			r1c2 = Integer.parseInt(subparts[1]);
			op1 = Integer.parseInt(parts[1]);
			line = scan.nextLine();
			line = scan.nextLine();
			parts = line.trim().split(" ");
			table3 = Integer.parseInt(parts[0].substring(1, 2));
			subparts = parts[0].trim().split("_");
			r2c1 = Integer.parseInt(subparts[1]);
			table4 = Integer.parseInt(parts[2].substring(1, 2));
			subparts = parts[2].trim().split("_");
			r2c2 = Integer.parseInt(subparts[1]);
			op2 = Integer.parseInt(parts[1]);
			
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		
		//sets the number to the appropriate opcode
		if(op1 == 1){
			op1 = AttrOperator.aopLT;
		}
		else{
			op1 = AttrOperator.aopGT;
		}
		if(op2 == 1){
			op2 = AttrOperator.aopLT;
		}
		else{
			op2 = AttrOperator.aopGT;
		}
		//gets the total of the join estimate of the two tuples
		int result = Histogram.estimatejoins(hist[table1-1], r1c1, hist[table2-1], r1c2, op1, 
				hist[table3-1], r2c1, hist[table4-1], r2c2, op2);
		//calculates the selectivity as a percentage of the maximum possible output 
		int maxsize = hist[table1-1].get_size() * hist[table2-1].get_size();
		int selectivity = (result * 100) / maxsize;
		
		//print results
		System.out.println("estimate = " + result);
		System.out.println("selectivity = " + selectivity + "% of " + maxsize);
	}
}
