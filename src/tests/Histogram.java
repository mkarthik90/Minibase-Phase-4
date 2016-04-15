package tests;

import java.util.ArrayList;
import java.util.Comparator;

public class Histogram {
	private int numrow;
	private int numcol;
	private long intervals[][];
	public static final int RESOLUTION = 200;
	private int num_intervals;
	
	public Histogram(int nrow, int ncol)
	{
		numrow = nrow;
		numcol = ncol;
		num_intervals = (numrow / RESOLUTION) + 1;
		intervals = new long[numcol][num_intervals];
		for(int i = 0; i < numcol; i++)
		{
			for(int j = 0; j < num_intervals; j++)
			{
				intervals[i][j] = 0;
			}
		}
	}
	
	public void build_hist(ArrayList rela, int num)
	{
		ArrayList<Long> col = null;
		Comp com = new Comp();
		switch(num)
		{
		case 1:
			ArrayList<TableEntry1> rel1 = (ArrayList<TableEntry1>)rela;
			//first-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel1.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel1.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel1.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel1.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).longValue();
			break;
		case 2:
			ArrayList<TableEntry2> rel2 = (ArrayList<TableEntry2>)rela;
			//first-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel2.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel2.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel2.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel2.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//fifth-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel2.get(i).rel5));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[4][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//sixth--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel2.get(i).rel6));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[5][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//seventh--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel2.get(i).rel7));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[6][num_intervals - 1] = col.get(col.size() - 1).longValue();
			break;
		case 3:
			ArrayList<TableEntry3> rel3 = (ArrayList<TableEntry3>)rela;

			//first-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel3.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel3.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel3.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel3.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//fifth-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel3.get(i).rel5));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[4][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//sixth--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel3.get(i).rel6));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[5][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//seventh--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel3.get(i).rel7));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[6][num_intervals - 1] = col.get(col.size() - 1).longValue();
			break;
		case 4:
			ArrayList<TableEntry4> rel4 = (ArrayList<TableEntry4>) rela;
			//first-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel4.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel4.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel4.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//fourth-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel4.get(i).rel4));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[3][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[3][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//fifth-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel4.get(i).rel5));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[4][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[4][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//sixth--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel4.get(i).rel6));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[5][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[5][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//seventh--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel4.get(i).rel7));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[6][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[6][num_intervals - 1] = col.get(col.size() - 1).longValue();
			break;
		case 5:
			ArrayList<TableEntry5> rel5 = (ArrayList<TableEntry5>) rela;

			//first-------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel5.get(i).rel1));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[0][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[0][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//second--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel5.get(i).rel2));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[1][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[1][num_intervals - 1] = col.get(col.size() - 1).longValue();
	
			//third--------------------------------------------------------------------
			col = new ArrayList<Long>();
			for(int i = 0; i < numrow; i++)
			{
				col.add(new Long(rel5.get(i).rel3));
			}
			col.sort(com);
			for(int i = 0; i < num_intervals - 1; i++)
			{
				intervals[2][i] = col.get(i * RESOLUTION).longValue();
			}
			intervals[2][num_intervals - 1] = col.get(col.size() - 1).longValue();
			break;
		}
	}
	
	public long[] view_hist(int col)
	{
		if(col <= numcol && col > 0)
		{
			return intervals[col - 1];
		}
		return null;
	}
	
	private class Comp implements Comparator<Long>
	{
		public int compare(Long l, Long r)
		{
			if(l.longValue() < r.longValue())
				return 1;
			else if(l.longValue() > r.longValue())
				return -1;
			else
				return 0;
		}
	}
}
