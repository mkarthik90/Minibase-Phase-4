package tests;
import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.ByteBuffer;

import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

public class BuilderTest {
	public static void main(String args[])
	{
		Histogram[] hist = DBBuilderP4.build_sorted();
		
		for(int i = 0; i < 5; i++)
		{
			int[] measures = hist[i].view_hist(3);
			System.out.print("{ ");
			System.out.print(measures[0]);
			for(int j = 1; j < measures.length; j++)
			{
				System.out.print(", " + measures[j]);
			}
			System.out.println(" }");
		}
		
	    System.out.println("\nFIN");
	}
}
