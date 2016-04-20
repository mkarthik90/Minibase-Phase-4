package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bufmgr.PageNotReadException;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import parser.Query;
import parser.QueryPred;
import parser.QueryRel;
import tests.DBBuilderP4;

public class IEJoinInMemoryP4 {
	private Map<String, Set<Integer>> _allProjRels;
	public static Map<String, Map<Integer, Integer>> intermediateCols;
	private int _currIntCol, _numTuples;

	public IEJoinInMemoryP4(Query query, boolean onDisk) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		Set<String> inIntermediate = new HashSet<String>();
		_currIntCol = 1;
		intermediateCols = new LinkedHashMap<String, Map<Integer, Integer>>();
		_populateProjRels(query);
		_setIntCols();

		QueryPred pred1, pred2;
		QueryRel leftIntRel, rightIntRel;
		Iterator<QueryPred> it = query.where.iterator();

		pred1 = it.next();
		inIntermediate.add(pred1.leftRel.table);
		inIntermediate.add(pred1.rightRel.table);

		if(it.hasNext()){
			pred2 = it.next();
			inIntermediate.add(pred2.leftRel.table);
			inIntermediate.add(pred2.rightRel.table);
		}
		else{
			throw new IllegalArgumentException("Not enough predicates in query.");
		}

		IEJoinInMemory join = IEJoinInMemory.fromPred(pred1, pred2, _allProjRels);
		join.writeResult();

		while(it.hasNext()){
			pred1 = it.next();

			/*
			if(it.hasNext()){
				pred2 = it.next();
			}
			else{
				throw new IllegalArgumentException("Not enough predicates in query.");
			}
			 */

			int col1, col2;

			col1 = getIntCol(pred1.leftRel);
			leftIntRel = new QueryRel("intermediate", col1);
			pred1.leftRel = leftIntRel;


			if (inIntermediate.contains(pred1.rightRel.table)){
				col2 = getIntCol(pred1.rightRel);
				rightIntRel = new QueryRel("intermediate", col2);
				pred1.rightRel = rightIntRel;

				_numTuples = compareIntermediateTuples(col1, col2, pred1.op.attrOperator);
			}
			else{
				if(!pred1.rightRel.table.equals("intermediate")){
					inIntermediate.add(pred1.rightRel.table);
				}

				if(!pred1.leftRel.table.equals("intermediate")){
					inIntermediate.add(pred1.leftRel.table);
				}

				join = IEJoinInMemory.fromPred(pred1, pred1.clone(), _allProjRels);
				_numTuples = join.writeResult();
			}

			/*
			col = getIntCol(pred2.leftRel);
			leftIntRel = new QueryRel("intermediate", col);
			pred2.leftRel = leftIntRel;
			 */

		}
	}
	
	public int getNumTuples(){
		return _numTuples;
	}

	private void _populateProjRels(Query query) {
		_allProjRels = new LinkedHashMap<String, Set<Integer>>();

		for(QueryRel rel : query.select){
			_addRelCol(rel);
		}

		for(QueryPred pred : query.where){
			_addRelCol(pred.leftRel);
			_addRelCol(pred.rightRel);
		}
	}

	private void _addRelCol(QueryRel rel){
		if(!_allProjRels.containsKey(rel.table)){
			_allProjRels.put(rel.table, new LinkedHashSet<Integer>());
		}

		_allProjRels.get(rel.table).add(rel.col);
	}

	private void _setIntCols(){
		for(String key : _allProjRels.keySet()){
			if(!intermediateCols.containsKey(key)){
				intermediateCols.put(key, new LinkedHashMap<Integer, Integer>());
			}

			for(Integer temp : _allProjRels.get(key)){
				intermediateCols.get(key).put(temp, _currIntCol);
				_currIntCol++;
			}
		}

		IEJoinInMemory.basicProjections.put("intermediate", new FldSpec[_currIntCol - 1]);
		IEJoinInMemory.attrTypes.put("intermediate", new AttrType[_currIntCol - 1]);
		IEJoinInMemory.tableColNums.put("intermediate", _currIntCol - 1);

		for(int i = 0; i < IEJoinInMemory.basicProjections.get("intermediate").length; i++){
			IEJoinInMemory.attrTypes.get("intermediate")[i] = new AttrType(AttrType.attrInteger);
			IEJoinInMemory.basicProjections.get("intermediate")[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
		}
	}

	public static int getIntCol(QueryRel rel){
		return getIntCol(rel.table, rel.col);
	}

	public static int getIntCol(String table, int col){
		int val = -1;

		if(intermediateCols.containsKey(table)){
			if(intermediateCols.get(table).containsKey(col)){
				val = intermediateCols.get(table).get(col);
			}
		}

		return val;
	}

	private static int compareIntermediateTuples(int col1, int col2, int op){
		int numTuples = 0;

		try {
			Tuple t;
			int val1, val2;
			List<Tuple> intTuples = new ArrayList<Tuple>();
			FileScan scan = new FileScan("intermediate.in", IEJoinInMemory.attrTypes.get("intermediate"), null, (short) IEJoinInMemory.tableColNums.get("intermediate").intValue(), (short) IEJoinInMemory.tableColNums.get("intermediate").intValue(), IEJoinInMemory.basicProjections.get("intermediate"), null);

			while((t = scan.get_next()) != null){
				Tuple tempCopy = new Tuple();
				tempCopy = new Tuple();
				tempCopy.setHdr((short) IEJoinInMemory.tableColNums.get("intermediate").intValue(), IEJoinInMemory.attrTypes.get("intermediate"), null);
				tempCopy.tupleCopy(t);

				intTuples.add(tempCopy);
			}

			if(IEJoinInMemory.hFile != null){
				IEJoinInMemory.hFile.deleteFile();
			}

			IEJoinInMemory.hFile = DBBuilderP4.make_new_heap("intermediate");

			for(Tuple temp : intTuples){
				val1 = temp.getIntFld(col1);
				val2 = temp.getIntFld(col2);

				if(IEJoinInMemory.evaluate(val1, op, val2)){
					DBBuilderP4.insert_tuple(IEJoinInMemory.hFile, temp);
					numTuples++;
				}
			}

		} catch (FileScanException | TupleUtilsException | InvalidRelation | IOException | JoinsException | InvalidTupleSizeException | InvalidTypeException | PageNotReadException | PredEvalException | UnknowAttrType | FieldNumberOutOfBoundException | WrongPermat | InvalidSlotNumberException | FileAlreadyDeletedException | HFBufMgrException | HFDiskMgrException e) {
			e.printStackTrace();
			System.exit(0);
		}

		return numTuples;
	}
}
