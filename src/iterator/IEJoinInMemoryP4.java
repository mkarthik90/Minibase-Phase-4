package iterator;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import bufmgr.PageNotReadException;
import global.AttrType;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;
import parser.Query;
import parser.QueryPred;
import parser.QueryRel;

public class IEJoinInMemoryP4 {
	private Map<String, Set<Integer>> _allProjRels;
	public static Map<String, Map<Integer, Integer>> intermediateCols;
	private int _currIntCol;

	public IEJoinInMemoryP4(Query query, boolean onDisk) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		_currIntCol = 1;
		intermediateCols = new LinkedHashMap<String, Map<Integer, Integer>>();
		_populateProjRels(query);
		_setIntCols();

		QueryPred pred1, pred2;
		QueryRel intRel;
		Iterator<QueryPred> it = query.where.iterator();
		int col;

		pred1 = it.next();

		if(it.hasNext()){
			pred2 = it.next();
		}
		else{
			throw new IllegalArgumentException("Not enough predicates in query.");
		}

		IEJoinInMemory join = IEJoinInMemory.fromPred(pred1, pred2, _allProjRels);
		join.writeResult();

		while(it.hasNext()){
			pred1 = it.next();

			if(it.hasNext()){
				pred2 = it.next();
			}
			else{
				throw new IllegalArgumentException("Not enough predicates in query.");
			}

			col = getIntCol(pred1.leftRel);
			intRel = new QueryRel("intermediate", col);
			pred1.leftRel = intRel;

			col = getIntCol(pred2.leftRel);
			intRel = new QueryRel("intermediate", col);
			pred2.leftRel = intRel;

			join = IEJoinInMemory.fromPred(pred1, pred2, _allProjRels);
			join.writeResult();
		}
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
		return intermediateCols.get(rel.table).get(rel.col);
	}
	
	public static int getIntCol(String table, int col){
		return intermediateCols.get(table).get(col);
	}
}
