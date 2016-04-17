package tests;

import java.util.ArrayList;
import java.util.Comparator;

import heap.Tuple;

public class Histogram {
	private int numrow;
	private int numcol;
	private int intervals[][];
	public static final int RESOLUTION = 200;
	public static final int PERCENTAGE = 10;
	public static final int SAMPLE = 1;
	public static final int SORTED = 2;
	private int num_intervals;
	
	public Histogram(int nrow, int ncol, int samplesort)
	{
		numrow = nrow;
		numcol = ncol;
		if(samplesort == SORTED)
		{
			num_intervals = (numrow / RESOLUTION) + 1;
		}
		else
		{
			num_intervals = (numrow * PERCENTAGE) / 100;
		}
		intervals = new int[numcol][num_intervals];
		for(int i = 0; i < numcol; i++)
		{
			for(int j = 0; j < num_intervals; j++)
			{
				intervals[i][j] = 0;
			}
		}
	}
	
	public void build_sample_hist(ArrayList rela, int num)
	{
		ArrayList<Integer> col = null;
		switch(num)
		{
		case 1:
			ArrayList<TableEntry1> rel1 = (ArrayList<TableEntry1>)rela;
			//first-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = rel1.get(i).rel1;
			}
			intervals[0][num_intervals - 1] = rel1.get(rel1.size() - 1).rel1;
	
			//second--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = rel1.get(i).rel2;
			}
			intervals[1][num_intervals - 1] = rel1.get(rel1.size() - 1).rel2;
	
			//third--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = rel1.get(i).rel3;
			}
			intervals[2][num_intervals - 1] = rel1.get(rel1.size() - 1).rel3;
	
			//fourth-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = rel1.get(i).rel4;
			}
			intervals[3][num_intervals - 1] = rel1.get(rel1.size() - 1).rel4;
			break;
		case 2:
			ArrayList<TableEntry2> rel2 = (ArrayList<TableEntry2>)rela;
			//first-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = rel2.get(i).rel1;
			}
			intervals[0][num_intervals - 1] = rel2.get(rel2.size() - 1).rel1;
	
			//second--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = rel2.get(i).rel2;
			}
			intervals[1][num_intervals - 1] = rel2.get(rel2.size() - 1).rel2;
	
			//third--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = rel2.get(i).rel3;
			}
			intervals[2][num_intervals - 1] = rel2.get(rel2.size() - 1).rel3;
	
			//fourth-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = rel2.get(i).rel4;
			}
			intervals[3][num_intervals - 1] = rel2.get(rel2.size() - 1).rel4;
	
			//fifth-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = rel2.get(i).rel5;
			}
			intervals[4][num_intervals - 1] = rel2.get(rel2.size() - 1).rel5;
	
			//sixth--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = rel2.get(i).rel6;
			}
			intervals[5][num_intervals - 1] = rel2.get(rel2.size() - 1).rel6;
	
			//seventh--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = rel2.get(i).rel7;
			}
			intervals[6][num_intervals - 1] = rel2.get(rel2.size() - 1).rel7;
			break;
		case 3:
			ArrayList<TableEntry3> rel3 = (ArrayList<TableEntry3>)rela;
			//first-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = rel3.get(i).rel1;
			}
			intervals[0][num_intervals - 1] = rel3.get(rel3.size() - 1).rel1;
	
			//second--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = rel3.get(i).rel2;
			}
			intervals[1][num_intervals - 1] = rel3.get(rel3.size() - 1).rel2;
	
			//third--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = rel3.get(i).rel3;
			}
			intervals[2][num_intervals - 1] = rel3.get(rel3.size() - 1).rel3;
	
			//fourth-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = rel3.get(i).rel4;
			}
			intervals[3][num_intervals - 1] = rel3.get(rel3.size() - 1).rel4;
	
			//fifth-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = rel3.get(i).rel5;
			}
			intervals[4][num_intervals - 1] = rel3.get(rel3.size() - 1).rel5;
	
			//sixth--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = rel3.get(i).rel6;
			}
			intervals[5][num_intervals - 1] = rel3.get(rel3.size() - 1).rel6;
	
			//seventh--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = rel3.get(i).rel7;
			}
			intervals[6][num_intervals - 1] = rel3.get(rel3.size() - 1).rel7;
			break;
		case 4:
			ArrayList<TableEntry4> rel4 = (ArrayList<TableEntry4>) rela;
			//first-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = rel4.get(i).rel1;
			}
			intervals[0][num_intervals - 1] = rel4.get(rel4.size() - 1).rel1;
	
			//second--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = rel4.get(i).rel2;
			}
			intervals[1][num_intervals - 1] = rel4.get(rel4.size() - 1).rel2;
	
			//third--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = rel4.get(i).rel3;
			}
			intervals[2][num_intervals - 1] = rel4.get(rel4.size() - 1).rel3;
	
			//fourth-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = rel4.get(i).rel4;
			}
			intervals[3][num_intervals - 1] = rel4.get(rel4.size() - 1).rel4;
	
			//fifth-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = rel4.get(i).rel5;
			}
			intervals[4][num_intervals - 1] = rel4.get(rel4.size() - 1).rel5;
	
			//sixth--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = rel4.get(i).rel6;
			}
			intervals[5][num_intervals - 1] = rel4.get(rel4.size() - 1).rel6;
	
			//seventh--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = rel4.get(i).rel7;
			}
			intervals[6][num_intervals - 1] = rel4.get(rel4.size() - 1).rel7;
			break;
		case 5:
			ArrayList<TableEntry5> rel5 = (ArrayList<TableEntry5>) rela;

			//first-------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = rel5.get(i).rel1;
			}
			intervals[0][num_intervals - 1] = rel5.get(rel5.size() - 1).rel1;
	
			//second--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = rel5.get(i).rel2;
			}
			intervals[1][num_intervals - 1] = rel5.get(rel5.size() - 1).rel2;
	
			//third--------------------------------------------------------------------
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = rel5.get(i).rel3;
			}
			intervals[2][num_intervals - 1] = rel5.get(rel5.size() - 1).rel3;
			break;
		}
	}
	
	public void build_sorted_hist(ArrayList rela, int num)
	{
		ArrayList<Integer> col = null;
		Comp com = new Comp();
		switch(num)
		{
		case 1:
			ArrayList<TableEntry1> rel1 = (ArrayList<TableEntry1>)rela;
			//first-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel1.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel1.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel1.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel1.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).intValue();
			break;
		case 2:
			ArrayList<TableEntry2> rel2 = (ArrayList<TableEntry2>)rela;
			//first-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel2.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel2.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel2.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel2.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//fifth-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel2.get(i).rel5));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[4][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//sixth--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel2.get(i).rel6));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[5][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//seventh--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel2.get(i).rel7));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[6][num_intervals - 1] = col.get(col.size() - 1).intValue();
			break;
		case 3:
			ArrayList<TableEntry3> rel3 = (ArrayList<TableEntry3>)rela;

			//first-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel3.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel3.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel3.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel3.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//fifth-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel3.get(i).rel5));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[4][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//sixth--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel3.get(i).rel6));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[5][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//seventh--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel3.get(i).rel7));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[6][num_intervals - 1] = col.get(col.size() - 1).intValue();
			break;
		case 4:
			ArrayList<TableEntry4> rel4 = (ArrayList<TableEntry4>) rela;
			//first-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel4.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel4.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel4.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel4.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//fifth-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel4.get(i).rel5));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[4][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//sixth--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel4.get(i).rel6));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[5][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//seventh--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel4.get(i).rel7));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[6][num_intervals - 1] = col.get(col.size() - 1).intValue();
			break;
		case 5:
			ArrayList<TableEntry5> rel5 = (ArrayList<TableEntry5>) rela;

			//first-------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel5.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel5.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).intValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Integer>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Integer(rel5.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).intValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).intValue();
			break;
		}
	}
	
	public int[] view_hist(int col)
	{
		if(col <= numcol && col > 0)
		{
			return intervals[col - 1];
		}
		return null;
	}
	
	public int get_size()
	{
		return numrow;
	}
	
	private class Comp implements Comparator<Integer>
	{
		public int compare(Integer l, Integer r)
		{
			if(l.intValue() < r.intValue())
				return 1;
			else if(l.intValue() > r.intValue())
				return -1;
			else
				return 0;
		}
	}
}
