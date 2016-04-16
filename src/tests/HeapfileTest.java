package tests;

import java.io.IOException;

import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;

public class HeapfileTest {
	public static void main(String[] args) throws HFException, HFBufMgrException, HFDiskMgrException, IOException{
		DBBuilderP4.build();
		
		Heapfile file = DBBuilderP4.make_new_heap("F1'");
		Heapfile file1 = DBBuilderP4.make_new_heap("F1'");
		Heapfile file2 = DBBuilderP4.make_new_heap("F1'");
		Heapfile file3 = DBBuilderP4.make_new_heap("F1'");
	}
}
