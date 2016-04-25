package tests;

import java.util.ArrayList;
import java.util.Comparator;

import global.AttrOperator;
import heap.Tuple;

public class Histogram {
	private int numrow;
	private int numcol;
	private int intervals[][];
	private int resolution;
	public static final int PERCENTAGE = 10;
	public static final int SAMPLE = 1;
	public static final int SORTED = 2;
	private int num_intervals;
	
	public Histogram(int nrow, int ncol, int samplesort)
	{
		numrow = nrow;
		numcol = ncol;
		num_intervals = ((numrow * PERCENTAGE) / 100) + 1;
		resolution = (numrow / (num_intervals - 1));
		intervals = new int[numcol][num_intervals];
		for(int i = 0; i < numcol; i++)
		{
			for(int j = 0; j < num_intervals; j++)
			{
				intervals[i][j] = 0;
			}
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
				intervals[0][i] = col.get(i * resolution).intValue();
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
				intervals[1][i] = col.get(i * resolution).intValue();
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
				intervals[2][i] = col.get(i * resolution).intValue();
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
				intervals[3][i] = col.get(i * resolution).intValue();
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
				intervals[0][i] = col.get(i * resolution).intValue();
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
				intervals[1][i] = col.get(i * resolution).intValue();
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
				intervals[2][i] = col.get(i * resolution).intValue();
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
				intervals[3][i] = col.get(i * resolution).intValue();
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
				intervals[4][i] = col.get(i * resolution).intValue();
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
				intervals[5][i] = col.get(i * resolution).intValue();
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
				intervals[6][i] = col.get(i * resolution).intValue();
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
				intervals[0][i] = col.get(i * resolution).intValue();
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
				intervals[1][i] = col.get(i * resolution).intValue();
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
				intervals[2][i] = col.get(i * resolution).intValue();
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
				intervals[3][i] = col.get(i * resolution).intValue();
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
				intervals[4][i] = col.get(i * resolution).intValue();
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
				intervals[5][i] = col.get(i * resolution).intValue();
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
				intervals[6][i] = col.get(i * resolution).intValue();
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
				intervals[0][i] = col.get(i * resolution).intValue();
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
				intervals[1][i] = col.get(i * resolution).intValue();
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
				intervals[2][i] = col.get(i * resolution).intValue();
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
				intervals[3][i] = col.get(i * resolution).intValue();
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
				intervals[4][i] = col.get(i * resolution).intValue();
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
				intervals[5][i] = col.get(i * resolution).intValue();
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
				intervals[6][i] = col.get(i * resolution).intValue();
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
				intervals[0][i] = col.get(i * resolution).intValue();
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
				intervals[1][i] = col.get(i * resolution).intValue();
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
				intervals[2][i] = col.get(i * resolution).intValue();
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
	
	public int greater_than(int num, int col)
	{
		int i = 1;
		if(intervals[col-1][0] > num)
		{
			return numrow;
		}
		for(; i < num_intervals - 1; i++)
		{
			if(intervals[col - 1][i] > num)
			{
				int morethan = numrow - (resolution * i);
				int percentage = (int)(((double)(intervals[col-1][i] - num) / (double)(intervals[col-1][i] - intervals[col-1][i-1])) * resolution);
				return morethan + percentage;
			}
		}
		if(intervals[col-1][num_intervals - 1] > num)
		{
			return (int)(((double)(intervals[col-1][num_intervals - 1] - num) / (double)(intervals[col-1][num_intervals - 1] - intervals[col-1][num_intervals - 2])) * resolution);
		}
		return 0;
	}
	
	public int less_than(int num, int col)
	{
		if(intervals[col-1][num_intervals - 1] < num)
		{
			return numrow;
		}
		for(int i = num_intervals - 2; i > 0; i--)
		{
			if(intervals[col - 1][i] < num)
			{
				int lessthan = resolution * i;
				int percentage = (int)(((double)(num - intervals[col-1][i]) / (double)(intervals[col-1][i] - intervals[col-1][i-1])) * resolution);
				return lessthan + percentage;
			}
		}
		if(intervals[col-1][0] < num)
		{
			return (int)(((double)(num - intervals[col-1][0]) / (double)(intervals[col-1][1] - intervals[col-1][0])) * resolution);
		}
		return 0;
	}
	
	public static int estimatejoins(Histogram hist11, int col11, Histogram hist12, int col12, int op1, Histogram hist21, int col21, Histogram hist22, int col22, int op2)
	{
		int select1 = estimatejoin(hist11, col11, hist12, col12, op1);
		int select2 = estimatejoin(hist21, col21, hist22, col22, op2);
		double per1 = (double)(select1) / (hist11.get_size() * hist12.get_size());
		double per2 = (double)(select2) / (hist21.get_size() * hist22.get_size());
		double pert = per1 * per2;
		
		return (int)(pert * hist11.get_size() * hist12.get_size());
	}
	
	public static int estimatejoin(Histogram hist1, int col1, Histogram hist2, int col2, int op)
	{
		int sum = 0;
		int h1[] = hist1.view_hist(col1);
		if(op == AttrOperator.aopGT)
		{
			for(int i = 0; i < h1.length - 2; i++)
			{
				sum += hist2.greater_than(h1[i], col2) * hist1.get_resolution();
			}
			sum += hist2.greater_than(h1[h1.length - 1], col2) * hist1.get_size_of_last();
		}
		else
		{
			for(int i = 0; i < h1.length - 1; i++)
			{
				sum += hist2.less_than(h1[i], col2) * hist1.get_resolution();
			}
			sum += hist2.less_than(h1[h1.length - 1], col2) * hist1.get_size_of_last();
		}
		return sum;
	}
	
	public int get_resolution()
	{
		return resolution;
	}
	
	public int get_size_of_last()
	{
		return numrow - (resolution * (num_intervals - 1));
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
				return -1;
			else if(l.intValue() > r.intValue())
				return 1;
			else
				return 0;
		}
	}
}
