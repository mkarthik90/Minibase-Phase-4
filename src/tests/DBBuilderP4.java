package tests;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;

class TableEntry1 {
	public final static String c1 = "id";
	public final static String c2 = "dept";
	public final static String c3 = "salary";
	public final static String c4 = "tax";
	public final static AttrType at[] = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	public final static FldSpec fs[] = {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3), new FldSpec(new RelSpec(RelSpec.outer), 4)};
	public int rel1;
	public int rel2;
	public int rel3;
	public int rel4;

	public TableEntry1 (int _r1, int _r2, int _r3, int _r4) {
		rel1 = _r1;
		rel2 = _r2;
		rel3 = _r3;
		rel4 = _r4;
	}
}
class TableEntry2 {
	public final static String c1 = "id";
	public final static String c2 = "dept";
	public final static String c3 = "salary";
	public final static String c4 = "tax";
	public final static String c5 = "id2";
	public final static String c6 = "start";
	public final static String c7 = "end";
	public final static AttrType at[] = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
		new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	public final static FldSpec fs[] = {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
		new FldSpec(new RelSpec(RelSpec.outer), 4), new FldSpec(new RelSpec(RelSpec.outer), 5), new FldSpec(new RelSpec(RelSpec.outer), 6), new FldSpec(new RelSpec(RelSpec.outer), 7)};
	public int rel1;
	public int rel2;
	public int rel3;
	public int rel4;
	public int rel5;
	public int rel6;
	public int rel7;

	public TableEntry2 (int _r1, int _r2, int _r3, int _r4, int _r5, int _r6, int _r7) {
		rel1 = _r1;
		rel2 = _r2;
		rel3 = _r3;
		rel4 = _r4;
		rel5 = _r5;
		rel6 = _r6;
		rel7 = _r7;
	}
}
class TableEntry3 {
	public final static String c1 = "id2";
	public final static String c2 = "start";
	public final static String c3 = "end";
	public final static String c4 = "id";
	public final static String c5 = "dept";
	public final static String c6 = "salary";
	public final static String c7 = "tax";
	public final static AttrType at[] = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
			new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	public final static FldSpec fs[] = {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
			new FldSpec(new RelSpec(RelSpec.outer), 4), new FldSpec(new RelSpec(RelSpec.outer), 5), new FldSpec(new RelSpec(RelSpec.outer), 6), new FldSpec(new RelSpec(RelSpec.outer), 7)};
	public int rel1;
	public int rel2;
	public int rel3;
	public int rel4;
	public int rel5;
	public int rel6;
	public int rel7;

	public TableEntry3 (int _r1, int _r2, int _r3, int _r4, int _r5, int _r6, int _r7) {
		rel1 = _r1;
		rel2 = _r2;
		rel3 = _r3;
		rel4 = _r4;
		rel5 = _r5;
		rel6 = _r6;
		rel7 = _r7;
	}
}
class TableEntry4 {
	public final static String c1 = "id";
	public final static String c2 = "dept";
	public final static String c3 = "salary";
	public final static String c4 = "tax";
	public final static String c5 = "id2";
	public final static String c6 = "start";
	public final static String c7 = "end";
	public final static AttrType at[] = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
		new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	public final static FldSpec fs[] = {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
		new FldSpec(new RelSpec(RelSpec.outer), 4), new FldSpec(new RelSpec(RelSpec.outer), 5), new FldSpec(new RelSpec(RelSpec.outer), 6), new FldSpec(new RelSpec(RelSpec.outer), 7)};
	public int rel1;
	public int rel2;
	public int rel3;
	public int rel4;
	public int rel5;
	public int rel6;
	public int rel7;

	public TableEntry4 (int _r1, int _r2, int _r3, int _r4, int _r5, int _r6, int _r7) {
		rel1 = _r1;
		rel2 = _r2;
		rel3 = _r3;
		rel4 = _r4;
		rel5 = _r5;
		rel6 = _r6;
		rel7 = _r7;
	}
}
class TableEntry5 {
	public final static String c1 = "id2";
	public final static String c2 = "start";
	public final static String c3 = "end";
	public final static AttrType at[] = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	public final static FldSpec fs[] = {new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3)};
	public int rel1;
	public int rel2;
	public int rel3;

	public TableEntry5 (int _r1, int _r2, int _r3) {
		rel1 = _r1;
		rel2 = _r2;
		rel3 = _r3;
	}
}





public class DBBuilderP4 implements GlobalConst{
	private static ArrayList<TableEntry1> RTable1;
	private static ArrayList<TableEntry2> RTable2;
	private static ArrayList<TableEntry3> RTable3;
	private static ArrayList<TableEntry4> RTable4;
	private static ArrayList<TableEntry5> RTable5;
	private static final String fn1 = "F1NR";
	private static final String fn2 = "F2NR";
	private static final String fn3 = "F3NR";
	private static final String fn4 = "F4NR";
	private static final String fn5 = "F5NR";
	
	
	public static int[] build()
	{
		RTable1 = new ArrayList<TableEntry1>();
		RTable2 = new ArrayList<TableEntry2>();
		RTable3 = new ArrayList<TableEntry3>();
		RTable4 = new ArrayList<TableEntry4>();
		RTable5 = new ArrayList<TableEntry5>();
		Histogram hist[] = new Histogram[5];
		String fndb = "datasetsPhase4/";
		String line;
		String parts[];
		int r1 = 0;
		int r2 = 0;
		int r3 = 0;
		int r4 = 0;
		int r5 = 0;
		int r6 = 0;
		int r7 = 0;
		TableEntry1 te1;
		TableEntry2 te2;
		TableEntry3 te3;
		TableEntry4 te4;
		TableEntry5 te5;
		
		//read files=====================================================================================================
		//F1----------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn1 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				te1 = new TableEntry1(r1,r2,r3,r4);
				RTable1.add(te1);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}


		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}
		new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

		short [] Ssizes = null;

		Tuple t = new Tuple();

		try {
			t.setHdr((short) 4,TableEntry1.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		int size = t.size();

		Heapfile        f = null;
		try {
			f = new Heapfile(fn1 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, TableEntry1.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable1.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry1)RTable1.get(i)).rel1);
				t.setIntFld(2, ((TableEntry1)RTable1.get(i)).rel2);
				t.setIntFld(3, ((TableEntry1)RTable1.get(i)).rel3);
				t.setIntFld(4, ((TableEntry1)RTable1.get(i)).rel4);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		//F2--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn2 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te2 = new TableEntry2(r1,r2,r3,r4,r5,r6,r7);
				RTable2.add(te2);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}

		t = new Tuple();

		try {
			t.setHdr((short) 7,TableEntry2.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn2 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry2.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable2.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry2)RTable2.get(i)).rel1);
				t.setIntFld(2, ((TableEntry2)RTable2.get(i)).rel2);
				t.setIntFld(3, ((TableEntry2)RTable2.get(i)).rel3);
				t.setIntFld(4, ((TableEntry2)RTable2.get(i)).rel4);
				t.setIntFld(5, ((TableEntry2)RTable2.get(i)).rel5);
				t.setIntFld(6, ((TableEntry2)RTable2.get(i)).rel6);
				t.setIntFld(7, ((TableEntry2)RTable2.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}	
		
		//F3--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn3 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te3 = new TableEntry3(r1,r2,r3,r4,r5,r6,r7);
				RTable3.add(te3);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}


		t = new Tuple();

		try {
			t.setHdr((short) 7, TableEntry3.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn3 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry3.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable3.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry3)RTable3.get(i)).rel1);
				t.setIntFld(2, ((TableEntry3)RTable3.get(i)).rel2);
				t.setIntFld(3, ((TableEntry3)RTable3.get(i)).rel3);
				t.setIntFld(4, ((TableEntry3)RTable3.get(i)).rel4);
				t.setIntFld(5, ((TableEntry3)RTable3.get(i)).rel5);
				t.setIntFld(6, ((TableEntry3)RTable3.get(i)).rel6);
				t.setIntFld(7, ((TableEntry3)RTable3.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		//F4--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn4 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te4 = new TableEntry4(r1,r2,r3,r4,r5,r6,r7);
				RTable4.add(te4);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}

		t = new Tuple();

		try {
			t.setHdr((short) 7, TableEntry4.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn4 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry4.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable4.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry4)RTable4.get(i)).rel1);
				t.setIntFld(2, ((TableEntry4)RTable4.get(i)).rel2);
				t.setIntFld(3, ((TableEntry4)RTable4.get(i)).rel3);
				t.setIntFld(4, ((TableEntry4)RTable4.get(i)).rel4);
				t.setIntFld(5, ((TableEntry4)RTable4.get(i)).rel5);
				t.setIntFld(6, ((TableEntry4)RTable4.get(i)).rel6);
				t.setIntFld(7, ((TableEntry4)RTable4.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}

		//F5--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn5 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				te5 = new TableEntry5(r1,r2,r3);
				RTable5.add(te5);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}
		
		t = new Tuple();

		try {
			t.setHdr((short) 3, TableEntry5.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn5 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 3, TableEntry5.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable5.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry5)RTable5.get(i)).rel1);
				t.setIntFld(2, ((TableEntry5)RTable5.get(i)).rel2);
				t.setIntFld(3, ((TableEntry5)RTable5.get(i)).rel3);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		int sizes[] = new int[5];
		sizes[0] = RTable1.size();
		sizes[1] = RTable2.size();
		sizes[2] = RTable3.size();
		sizes[3] = RTable4.size();
		sizes[4] = RTable5.size();
		return sizes;
	}
	
	public static Histogram[] build_sample()
	{
		RTable1 = new ArrayList<TableEntry1>();
		RTable2 = new ArrayList<TableEntry2>();
		RTable3 = new ArrayList<TableEntry3>();
		RTable4 = new ArrayList<TableEntry4>();
		RTable5 = new ArrayList<TableEntry5>();
		Histogram hist[] = new Histogram[5];
		String fndb = "datasetsPhase4/";
		String line;
		String parts[];
		int r1 = 0;
		int r2 = 0;
		int r3 = 0;
		int r4 = 0;
		int r5 = 0;
		int r6 = 0;
		int r7 = 0;
		TableEntry1 te1;
		TableEntry2 te2;
		TableEntry3 te3;
		TableEntry4 te4;
		TableEntry5 te5;
		
		//read files=====================================================================================================
		//F1----------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn1 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				te1 = new TableEntry1(r1,r2,r3,r4);
				RTable1.add(te1);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}


		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}
		new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

		short [] Ssizes = null;

		Tuple t = new Tuple();

		try {
			t.setHdr((short) 4,TableEntry1.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		int size = t.size();

		Heapfile        f = null;
		try {
			f = new Heapfile(fn1 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, TableEntry1.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable1.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry1)RTable1.get(i)).rel1);
				t.setIntFld(2, ((TableEntry1)RTable1.get(i)).rel2);
				t.setIntFld(3, ((TableEntry1)RTable1.get(i)).rel3);
				t.setIntFld(4, ((TableEntry1)RTable1.get(i)).rel4);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		//F2--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn2 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te2 = new TableEntry2(r1,r2,r3,r4,r5,r6,r7);
				RTable2.add(te2);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}

		t = new Tuple();

		try {
			t.setHdr((short) 7,TableEntry2.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn2 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry2.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable2.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry2)RTable2.get(i)).rel1);
				t.setIntFld(2, ((TableEntry2)RTable2.get(i)).rel2);
				t.setIntFld(3, ((TableEntry2)RTable2.get(i)).rel3);
				t.setIntFld(4, ((TableEntry2)RTable2.get(i)).rel4);
				t.setIntFld(5, ((TableEntry2)RTable2.get(i)).rel5);
				t.setIntFld(6, ((TableEntry2)RTable2.get(i)).rel6);
				t.setIntFld(7, ((TableEntry2)RTable2.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}	
		
		//F3--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn3 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te3 = new TableEntry3(r1,r2,r3,r4,r5,r6,r7);
				RTable3.add(te3);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}


		t = new Tuple();

		try {
			t.setHdr((short) 7, TableEntry3.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn3 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry3.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable3.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry3)RTable3.get(i)).rel1);
				t.setIntFld(2, ((TableEntry3)RTable3.get(i)).rel2);
				t.setIntFld(3, ((TableEntry3)RTable3.get(i)).rel3);
				t.setIntFld(4, ((TableEntry3)RTable3.get(i)).rel4);
				t.setIntFld(5, ((TableEntry3)RTable3.get(i)).rel5);
				t.setIntFld(6, ((TableEntry3)RTable3.get(i)).rel6);
				t.setIntFld(7, ((TableEntry3)RTable3.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		//F4--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn4 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te4 = new TableEntry4(r1,r2,r3,r4,r5,r6,r7);
				RTable4.add(te4);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}

		t = new Tuple();

		try {
			t.setHdr((short) 7, TableEntry4.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn4 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry4.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable4.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry4)RTable4.get(i)).rel1);
				t.setIntFld(2, ((TableEntry4)RTable4.get(i)).rel2);
				t.setIntFld(3, ((TableEntry4)RTable4.get(i)).rel3);
				t.setIntFld(4, ((TableEntry4)RTable4.get(i)).rel4);
				t.setIntFld(5, ((TableEntry4)RTable4.get(i)).rel5);
				t.setIntFld(6, ((TableEntry4)RTable4.get(i)).rel6);
				t.setIntFld(7, ((TableEntry4)RTable4.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}

		//F5--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn5 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				te5 = new TableEntry5(r1,r2,r3);
				RTable5.add(te5);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}
		
		t = new Tuple();

		try {
			t.setHdr((short) 3, TableEntry5.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn5 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 3, TableEntry5.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable5.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry5)RTable5.get(i)).rel1);
				t.setIntFld(2, ((TableEntry5)RTable5.get(i)).rel2);
				t.setIntFld(3, ((TableEntry5)RTable5.get(i)).rel3);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}

		//create histograms====================================================================================
		hist[0] = new Histogram(RTable1.size(), 4, Histogram.SAMPLE);
		hist[1] = new Histogram(RTable2.size(), 7, Histogram.SAMPLE);
		hist[2] = new Histogram(RTable3.size(), 7, Histogram.SAMPLE);
		hist[3] = new Histogram(RTable4.size(), 7, Histogram.SAMPLE);
		hist[4] = new Histogram(RTable5.size(), 3, Histogram.SAMPLE);

		hist[0].build_sample_hist(RTable1, 1);
		hist[1].build_sample_hist(RTable2, 2);
		hist[2].build_sample_hist(RTable3, 3);
		hist[3].build_sample_hist(RTable4, 4);
		hist[4].build_sample_hist(RTable5, 5);
		
		return hist;
	}
	
	public static Histogram[] build_sorted(){
		RTable1 = new ArrayList<TableEntry1>();
		RTable2 = new ArrayList<TableEntry2>();
		RTable3 = new ArrayList<TableEntry3>();
		RTable4 = new ArrayList<TableEntry4>();
		RTable5 = new ArrayList<TableEntry5>();
		Histogram hist[] = new Histogram[5];
		String fndb = "datasetsPhase4/";
		String line;
		String parts[];
		int r1 = 0;
		int r2 = 0;
		int r3 = 0;
		int r4 = 0;
		int r5 = 0;
		int r6 = 0;
		int r7 = 0;
		TableEntry1 te1;
		TableEntry2 te2;
		TableEntry3 te3;
		TableEntry4 te4;
		TableEntry5 te5;
		
		//read files=====================================================================================================
		//F1----------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn1 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				te1 = new TableEntry1(r1,r2,r3,r4);
				RTable1.add(te1);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}


		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}
		new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

		short [] Ssizes = null;

		Tuple t = new Tuple();

		try {
			t.setHdr((short) 4,TableEntry1.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		int size = t.size();

		Heapfile        f = null;
		try {
			f = new Heapfile(fn1 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, TableEntry1.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable1.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry1)RTable1.get(i)).rel1);
				t.setIntFld(2, ((TableEntry1)RTable1.get(i)).rel2);
				t.setIntFld(3, ((TableEntry1)RTable1.get(i)).rel3);
				t.setIntFld(4, ((TableEntry1)RTable1.get(i)).rel4);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		//F2--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn2 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te2 = new TableEntry2(r1,r2,r3,r4,r5,r6,r7);
				RTable2.add(te2);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}

		t = new Tuple();

		try {
			t.setHdr((short) 7,TableEntry2.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn2 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry2.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable2.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry2)RTable2.get(i)).rel1);
				t.setIntFld(2, ((TableEntry2)RTable2.get(i)).rel2);
				t.setIntFld(3, ((TableEntry2)RTable2.get(i)).rel3);
				t.setIntFld(4, ((TableEntry2)RTable2.get(i)).rel4);
				t.setIntFld(5, ((TableEntry2)RTable2.get(i)).rel5);
				t.setIntFld(6, ((TableEntry2)RTable2.get(i)).rel6);
				t.setIntFld(7, ((TableEntry2)RTable2.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}	
		
		//F3--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn3 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te3 = new TableEntry3(r1,r2,r3,r4,r5,r6,r7);
				RTable3.add(te3);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}


		t = new Tuple();

		try {
			t.setHdr((short) 7, TableEntry3.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn3 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry3.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable3.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry3)RTable3.get(i)).rel1);
				t.setIntFld(2, ((TableEntry3)RTable3.get(i)).rel2);
				t.setIntFld(3, ((TableEntry3)RTable3.get(i)).rel3);
				t.setIntFld(4, ((TableEntry3)RTable3.get(i)).rel4);
				t.setIntFld(5, ((TableEntry3)RTable3.get(i)).rel5);
				t.setIntFld(6, ((TableEntry3)RTable3.get(i)).rel6);
				t.setIntFld(7, ((TableEntry3)RTable3.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		//F4--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn4 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				r5 = Integer.parseInt(parts[4]);
				r6 = Integer.parseInt(parts[5]);
				r7 = Integer.parseInt(parts[6]);
				te4 = new TableEntry4(r1,r2,r3,r4,r5,r6,r7);
				RTable4.add(te4);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}

		t = new Tuple();

		try {
			t.setHdr((short) 7, TableEntry4.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn4 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 7, TableEntry4.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable4.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry4)RTable4.get(i)).rel1);
				t.setIntFld(2, ((TableEntry4)RTable4.get(i)).rel2);
				t.setIntFld(3, ((TableEntry4)RTable4.get(i)).rel3);
				t.setIntFld(4, ((TableEntry4)RTable4.get(i)).rel4);
				t.setIntFld(5, ((TableEntry4)RTable4.get(i)).rel5);
				t.setIntFld(6, ((TableEntry4)RTable4.get(i)).rel6);
				t.setIntFld(7, ((TableEntry4)RTable4.get(i)).rel7);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}

		//F5--------------------------------------------------------------------------------------
		try{
			FileReader fr = new FileReader(fndb + fn5 + ".csv");
			BufferedReader bufferedReader = new BufferedReader(fr);
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				te5 = new TableEntry5(r1,r2,r3);
				RTable5.add(te5);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}
		
		t = new Tuple();

		try {
			t.setHdr((short) 3, TableEntry5.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		size = t.size();

		f = null;
		try {
			f = new Heapfile(fn5 + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 3, TableEntry5.at, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable5.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry5)RTable5.get(i)).rel1);
				t.setIntFld(2, ((TableEntry5)RTable5.get(i)).rel2);
				t.setIntFld(3, ((TableEntry5)RTable5.get(i)).rel3);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		
		//create histograms====================================================================================
		hist[0] = new Histogram(RTable1.size(), 4, Histogram.SORTED);
		hist[1] = new Histogram(RTable2.size(), 7, Histogram.SORTED);
		hist[2] = new Histogram(RTable3.size(), 7, Histogram.SORTED);
		hist[3] = new Histogram(RTable4.size(), 7, Histogram.SORTED);
		hist[4] = new Histogram(RTable5.size(), 3, Histogram.SORTED);

		hist[0].build_sorted_hist(RTable1, 1);
		hist[1].build_sorted_hist(RTable2, 2);
		hist[2].build_sorted_hist(RTable3, 3);
		hist[3].build_sorted_hist(RTable4, 4);
		hist[4].build_sorted_hist(RTable5, 5);
		
		return hist;
	}
	
	
	public static Heapfile make_new_heap(String fn)
	{
		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}

		Tuple t = new Tuple();

		try{
			Runtime.getRuntime().exec("rm " + fn + ".in");
		}
		catch(Exception e)
		{}

		Heapfile        f = null;
		try {
			f = new Heapfile(fn + ".in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}
		
		return f;
	}
	
	public static void insert_tuple(Heapfile f, Tuple t)
	{
		try{ 
			f.insertRecord(t.getTupleByteArray());
		}
		catch(Exception e){
			e.printStackTrace();
			System.exit(0);;
		}
	}
}
