package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import parser.QueryPred;
import tests.DBBuilderP4;

public class IEJoinInMemory extends Iterator{
	public static List<Tuple> intermediateTable;
	public static Map<String, AttrType[]> attrTypes;
	public static Map<String, FldSpec[]> basicProjections;
	public static Map<String, Integer> tableColNums;
	static
	{
		basicProjections = new HashMap<String, FldSpec[]>();
		attrTypes = new HashMap<String, AttrType[]>();
		tableColNums = new HashMap<String, Integer>();
		tableColNums.put("R", 4);
		tableColNums.put("S", 4);
		tableColNums.put("Q", 4);
		tableColNums.put("F1NR", 4);
		tableColNums.put("F2NR", 7);
		tableColNums.put("F3NR", 7);
		tableColNums.put("F4NR", 7);
		tableColNums.put("F5NR", 3);

		for(String key : tableColNums.keySet()){
			attrTypes.put(key, new AttrType[tableColNums.get(key)]);
			basicProjections.put(key, new FldSpec[tableColNums.get(key)]);

			for(int i = 0; i < attrTypes.get(key).length; i++){
				basicProjections.get(key)[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
				attrTypes.get(key)[i] = new AttrType(AttrType.attrInteger);
			}
		}
	}

	private int[] l1Offset, l2Offset, bitArray, permArr, primePermArr;
	private int eqOff, m, n, _op1, _op2, _r1c1, _r1c2, _r2c1, _r2c2;
	private String _r1, _r2;
	private Map<String, Set<Integer>> _projRels;
	private IEJoinInMemoryArray _elements;
	public static Heapfile hFile = null;

	enum IEJoinInMemoryArrayType{
		L1, L1Prime, L2, L2Prime;

		public static final IEJoinInMemoryArrayType[] ALL_TYPES = {L1, L1Prime, L2, L2Prime};
	}

	class TupleComparator implements Comparator<Tuple>{
		private int _col;
		private int _order;

		public TupleComparator(int col, TupleOrder order){
			_col = col;
			_order = order.tupleOrder;
		}

		@Override
		public int compare(Tuple o1, Tuple o2) {
			int result = Integer.MAX_VALUE;

			try {
				if(_order == TupleOrder.Ascending){
					result = Integer.compare(o1.getIntFld(_col), o2.getIntFld(_col));
				}
				else{
					result = Integer.compare(o2.getIntFld(_col), o1.getIntFld(_col));
				}
			} catch (FieldNumberOutOfBoundException | IOException e) {
				e.printStackTrace();
			}

			return result;
		}

	}

	class IEJoinInMemoryArray{
		private Map<IEJoinInMemoryArrayType, List<Tuple>> _values;
		private int _l1TupleOrder, _l2TupleOrder;

		public IEJoinInMemoryArray(String r1, String r2, int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2){
			_values = new HashMap<IEJoinInMemoryArrayType, List<Tuple>>();
			_populateValues(r1, r2);
			_sortValues(r1c1, r2c1, r1c2, r2c2, op1, op2);
		}

		public int getR1Size(){
			return _values.get(IEJoinInMemoryArrayType.L1).size();
		}

		public int getR2Size(){
			return _values.get(IEJoinInMemoryArrayType.L1Prime).size();
		}

		public int getL1TupleOrder(){
			return _l1TupleOrder;
		}

		public int getL2TupleOrder(){
			return _l2TupleOrder;
		}

		private void _sortValues(int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2){
			TupleComparator r1Comp, r2Comp;

			//line 3-4
			if(op1 == AttrOperator.aopGT || op1 == AttrOperator.aopGE){
				//Sort in Descending Order
				r1Comp = new TupleComparator(r1c1, new TupleOrder(TupleOrder.Descending));
				r2Comp = new TupleComparator(r2c1, new TupleOrder(TupleOrder.Descending));
				_l1TupleOrder = TupleOrder.Descending;
			}
			else{
				//Sort in Ascending Order
				r1Comp = new TupleComparator(r1c1, new TupleOrder(TupleOrder.Ascending));
				r2Comp = new TupleComparator(r2c1, new TupleOrder(TupleOrder.Ascending));
				_l1TupleOrder = TupleOrder.Ascending;
			}

			Collections.sort(_values.get(IEJoinInMemoryArrayType.L1), r1Comp);
			Collections.sort(_values.get(IEJoinInMemoryArrayType.L1Prime), r2Comp);

			//line 5-6
			if(op2 == AttrOperator.aopGT || op2 == AttrOperator.aopGE){
				//Sort in Ascending Order
				r1Comp = new TupleComparator(r1c2, new TupleOrder(TupleOrder.Ascending));
				r2Comp = new TupleComparator(r2c2, new TupleOrder(TupleOrder.Ascending));
				_l2TupleOrder = TupleOrder.Ascending;
			}
			else{
				//Sort in Descending Order
				r1Comp = new TupleComparator(r1c2, new TupleOrder(TupleOrder.Descending));
				r2Comp = new TupleComparator(r2c2, new TupleOrder(TupleOrder.Descending));
				_l2TupleOrder = TupleOrder.Descending;
			}

			Collections.sort(_values.get(IEJoinInMemoryArrayType.L2), r1Comp);
			Collections.sort(_values.get(IEJoinInMemoryArrayType.L2Prime), r2Comp);
		}

		private void _populateValues(String r1, String r2){
			for(IEJoinInMemoryArrayType type : IEJoinInMemoryArrayType.ALL_TYPES){
				_values.put(type, new ArrayList<Tuple>());
			}

			try {
				Tuple temp, tempCopy;

				if(!r1.equals("intermediate")){
					FileScan r1Scan = new FileScan(r1 + ".in", attrTypes.get(r1), null, (short) tableColNums.get(r1).intValue(), (short) tableColNums.get(r1).intValue(), basicProjections.get(r1), null);
					while((temp = r1Scan.get_next()) != null){
						tempCopy = new Tuple();
						tempCopy.setHdr((short) tableColNums.get(r1).intValue(), attrTypes.get(r1), null);
						tempCopy.tupleCopy(temp);

						_values.get(IEJoinInMemoryArrayType.L1).add(tempCopy);
						_values.get(IEJoinInMemoryArrayType.L2).add(tempCopy);
					}
				}
				else{
					for(Tuple t : intermediateTable){
						_values.get(IEJoinInMemoryArrayType.L1).add(t);
						_values.get(IEJoinInMemoryArrayType.L2).add(t);
					}
				}

				if(!r2.equals("intermediate")){
					FileScan r2Scan = new FileScan(r2 + ".in", attrTypes.get(r2), null, (short) tableColNums.get(r2).intValue(), (short) tableColNums.get(r2).intValue(), basicProjections.get(r2), null);
					while((temp = r2Scan.get_next()) != null){
						tempCopy = new Tuple();
						tempCopy.setHdr((short) tableColNums.get(r2).intValue(), attrTypes.get(r2), null);
						tempCopy.tupleCopy(temp);

						_values.get(IEJoinInMemoryArrayType.L1Prime).add(tempCopy);
						_values.get(IEJoinInMemoryArrayType.L2Prime).add(tempCopy);
					}
				}
				else{
					for(Tuple t : intermediateTable){
						_values.get(IEJoinInMemoryArrayType.L1Prime).add(t);
						_values.get(IEJoinInMemoryArrayType.L2Prime).add(t);
					}
				}
				
				intermediateTable = new ArrayList<Tuple>();
			} catch (JoinsException | InvalidTupleSizeException | InvalidTypeException | PageNotReadException
					| PredEvalException | UnknowAttrType | FieldNumberOutOfBoundException | WrongPermat
					| IOException | FileScanException | TupleUtilsException | InvalidRelation e) {
				e.printStackTrace();
			}
		}

		public Tuple getFromL1(int idx){
			return _get(IEJoinInMemoryArrayType.L1, idx);
		}

		public Tuple getFromL1Prime(int idx){
			return _get(IEJoinInMemoryArrayType.L1Prime, idx);
		}

		public Tuple getFromL2(int idx){
			return _get(IEJoinInMemoryArrayType.L2, idx);
		}

		public Tuple getFromL2Prime(int idx){
			return _get(IEJoinInMemoryArrayType.L2Prime, idx);
		}

		public int getL2PrimeSize(){
			return _getSize(IEJoinInMemoryArrayType.L2Prime);
		}

		public int getL2Size(){
			return _getSize(IEJoinInMemoryArrayType.L2);
		}

		private int _getSize(IEJoinInMemoryArrayType type){
			return _values.get(type).size();
		}

		private Tuple _get(IEJoinInMemoryArrayType type, int idx){
			return _values.get(type).get(idx);
		}
	}

	public static IEJoinInMemory fromPred(QueryPred pred1, QueryPred pred2, Map<String, Set<Integer>> projRels) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		String r1, r2;
		int r1c1, r1c2, r2c1, r2c2, op1, op2;

		op1 = pred1.op.attrOperator;
		op2 = pred2.op.attrOperator;

		r1 = pred1.leftRel.table;
		r2 = pred1.rightRel.table;

		r1c1 = pred1.leftRel.col;
		r2c1 = pred1.rightRel.col;

		/*
		 * Check to see if order of tables is reversed in second predicate!
		 */
		if(pred2.leftRel.table.equals(r1)){
			r1c2 = pred2.leftRel.col;
			r2c2 = pred2.rightRel.col;
		}
		else{
			r1c2 = pred2.rightRel.col;
			r2c2 = pred2.leftRel.col;
		}

		return new IEJoinInMemory(r1, r2, r1c1, r2c1, r1c2, r2c2, op1, op2, projRels);
	}

	public IEJoinInMemory(String r1, String r2, int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2, Map<String, Set<Integer>> projRels) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		_projRels = projRels;
		_elements = new IEJoinInMemoryArray(r1, r2, r1c1, r2c1, r1c2, r2c2, op1, op2);
		_r1 = r1;
		_r2 = r2;
		_r1c1 = r1c1;
		_r1c2 = r1c2;
		_r2c1 = r2c1;
		_r2c2 = r2c2;
		_op1 = op1;
		_op2 = op2;
		m = _elements.getR1Size();
		n = _elements.getR2Size();

		//line 7
		permArr = new int[m];
		Tuple l2, l1;

		for(int i = 0; i < m; i++){
			l2 = _elements.getFromL2(i);

			for(int j = 0; j < m; j++){
				l1 = _elements.getFromL1(j);

				if(Arrays.equals(l2.getTupleByteArray(), l1.getTupleByteArray())){
					permArr[i] = j;
					break;
				}
			}
		}

		//line 8
		primePermArr = new int[n];
		Tuple l2Prime, l1Prime;

		for(int i = 0; i < n; i++){
			l2Prime = _elements.getFromL2Prime(i);

			for(int j = 0; j < n; j++){
				l1Prime = _elements.getFromL1Prime(j);

				if(Arrays.equals(l2Prime.getTupleByteArray(), l1Prime.getTupleByteArray())){
					primePermArr[i] = j;
					break;
				}
			}
		}

		//line 9

		l1Offset = new int[m];
		l2Offset = new int[m];
		boolean found;

		int l1PrimeVal, l1Val;

		for(int i = 0; i < m; i++){
			found = false;
			l1 = _elements.getFromL1(i);
			l1Val = l1.getIntFld(r1c1);

			for(int j = 0; j < n; j ++){
				l1Prime = _elements.getFromL1Prime(j);
				l1PrimeVal = l1Prime.getIntFld(r2c1);

				if(_elements.getL1TupleOrder() == TupleOrder.Ascending){
					if(l1Val <= l1PrimeVal){
						l1Offset[i] = j;
						found = true;
					}
				}
				else{
					if(l1Val >= l1PrimeVal){
						l1Offset[i] = j;
						found = true;
					}
				}

				if(found){
					break;
				}
				else{
					l1Offset[i] = j+1;
				}
			}
		}

		//line 10
		int l2PrimeVal, l2Val;

		for(int i = 0; i < m; i++){
			found = false;
			l2 = _elements.getFromL2(i);
			l2Val = l2.getIntFld(r1c2);

			for(int j = 0; j < n; j ++){
				l2Prime = _elements.getFromL2Prime(j);
				l2PrimeVal = l2Prime.getIntFld(r2c2);

				if(_elements.getL2TupleOrder() == TupleOrder.Ascending){
					if(l2Val <= l2PrimeVal){
						l2Offset[i] = j;
						found = true;
					}
				}
				else{
					if(l2Val >= l2PrimeVal){
						l2Offset[i] = j;
						found = true;
					}
				}

				if(found){
					break;
				}
				else{
					l2Offset[i] = j+1;
				}
			}
		}

		//line 11: initialize bit array B' (|B'| = n) and set all bits to 0
		bitArray = new int[n];

		//line 12

		//line 13-14
		if((op1 == AttrOperator.aopLE || op1 == AttrOperator.aopGE) || (op2 == AttrOperator.aopLE || op2 == AttrOperator.aopGE)){
			eqOff = 1;
		}
		else{
			eqOff = 0;
		}
	}

	public int getResult() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception{
		return this._getResult(false, "");
	}

	public int writeResult() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception{
		return this._getResult(true, "intermediate");
	}

	private int _getResult(boolean saveResults, String filename) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception{
		if(saveResults){
			if(hFile != null){
				hFile.deleteFile();
			}

			hFile = DBBuilderP4.make_new_heap(filename);
		}

		Tuple l1 = null, l1Prime = null, outTuple;
		int off2, off1, k, keyCol;

		Map<Integer, FldSpec> projMap = new HashMap<Integer, FldSpec>();
		FldSpec[] permMat;
		RelSpec spec;
		AttrType[] projAttrs; 

		if(!saveResults){
			for(String key : _projRels.keySet()){
				for(Integer col : _projRels.get(key)){
					if(key.equals(_r1)){
						spec = new RelSpec(RelSpec.outer);
					}
					else{
						spec = new RelSpec(RelSpec.innerRel);
					}

					projMap.put(col, new FldSpec(spec, col));
				}
			}
		}
		else{
			for(String key : _projRels.keySet()){
				for(Integer col : _projRels.get(key)){
					if((keyCol = IEJoinInMemoryP4.getIntCol(key, col)) == -1){
						keyCol = col;
					}

					if(key.equals(_r2)){
						spec = new RelSpec(RelSpec.innerRel);
					}
					else{
						spec = new RelSpec(RelSpec.outer);
					}

					if(_r1.equals("intermediate") && keyCol != col){
						projMap.put(col, new FldSpec(new RelSpec(RelSpec.outer), col));
					}

					projMap.put(keyCol, new FldSpec(spec, col));
				}
			}
		}

		permMat = new FldSpec[projMap.size()];

		for(Integer key : projMap.keySet()){
			permMat[key - 1] = projMap.get(key);
		}

		projAttrs = new AttrType[permMat.length];

		for(int i = 0; i < projAttrs.length; i++){
			projAttrs[i] = new AttrType(AttrType.attrInteger);
		}	

		int numTuples = 0;

		for(int i = 0; i < l2Offset.length; i++){

			off2 = l2Offset[i];

			for(int j = 0; j < off2; j++){
				int idx = primePermArr[j];

				if(idx < bitArray.length){
					bitArray[idx] = 1;
				}
			}

			off1 = l1Offset[permArr[i]];
			k = eqOff + off1;
			l1 = _elements.getFromL1(permArr[i]);

			while(k < n){
				l1Prime = _elements.getFromL1Prime(k);

				if(bitArray[k] == 1){
					Tuple out1 = new Tuple(), out2 = new Tuple();
					out1.setHdr((short) tableColNums.get(_r1).intValue(), attrTypes.get(_r1), null);
					out2.setHdr((short) tableColNums.get(_r2).intValue(), attrTypes.get(_r2), null);

					out1.tupleCopy(l1);
					out2.tupleCopy(l1Prime);

					int p1Out1 = out1.getIntFld(_r1c1), p1Out2 = out1.getIntFld(_r1c2),
							p2Out1 = out2.getIntFld(_r2c1), p2Out2 = out2.getIntFld(_r2c2);

					if(!evaluate(p1Out1, _op1, p2Out1) || !evaluate(p1Out2, _op2, p2Out2)){
						k++;
						continue;
					}

					outTuple = new Tuple();
					outTuple.setHdr((short)projAttrs.length, projAttrs, null);

					if(saveResults){
						combine(out1, out2, outTuple);
					}
					else{
						Projection.Join(out1, attrTypes.get(_r1), out2, attrTypes.get(_r2), outTuple, permMat, permMat.length);
					}





					if(!saveResults){
						outTuple.print(projAttrs);
					}
					else{
						intermediateTable.add(outTuple);
						//DBBuilderP4.insert_tuple(hFile, outTuple);
						//hFile.insertRecord(outTuple.returnTupleByteArray());
					}

					numTuples++;
				}

				k++;
			}
		}

		//System.out.println("Number of tuples: " + numTuples);
		return numTuples;
	}

	private void combine(Tuple t1, Tuple t2, Tuple out) throws FieldNumberOutOfBoundException, IOException{
		Set<Integer> r1Set, r2Set;
		int keyCol;

		for(int i = 0; i < out.noOfFlds(); i++){
			out.setIntFld(i+1, Integer.MIN_VALUE);
		}

		if(_r1.equals("intermediate")){
			for(String table : _projRels.keySet()){
				for(Integer col : _projRels.get(table)){
					keyCol = IEJoinInMemoryP4.getIntCol(table, col);
					out.setIntFld(keyCol, t1.getIntFld(keyCol));
				}
			}
		}
		else{
			r1Set = _projRels.get(_r1);

			for(Integer col : r1Set){
				keyCol = IEJoinInMemoryP4.getIntCol(_r1, col);
				out.setIntFld(keyCol, t1.getIntFld(col));
			}
		}

		r2Set = _projRels.get(_r2);

		for(Integer col : r2Set){
			keyCol = IEJoinInMemoryP4.getIntCol(_r2, col);

			out.setIntFld(keyCol, t2.getIntFld(col));
		}
	}

	public static boolean evaluate(int op1, int op, int op2) throws IllegalArgumentException{
		boolean valid;

		switch(op){
		case AttrOperator.aopLT:
			valid = op1 < op2;
			break;
		case AttrOperator.aopGT:
			valid = op1 > op2;
			break;
		case AttrOperator.aopGE:
			valid = op1 >= op2;
			break;
		case AttrOperator.aopLE:
			valid = op1 <= op2;
			break;
		case AttrOperator.aopEQ:
			valid = op1 == op2;
			break;
		default:
			throw new IllegalArgumentException("WHAAAAT");
		}

		return valid;
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
	InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
	LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub

	}
}
