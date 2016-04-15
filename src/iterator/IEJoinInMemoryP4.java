package iterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import bufmgr.PageNotReadException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;
import parser.Query;
import parser.QueryPred;
import parser.QueryRel;

public class IEJoinInMemoryP4 {
	private Map<String, Set<Integer>> _allProjRels;

	public IEJoinInMemoryP4(Query query, boolean onDisk) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		_populateProjRels(query);

		QueryPred pred, pred1, pred2;
		Iterator<QueryPred> it = query.where.iterator();

		pred1 = it.next();

		if(it.hasNext()){
			pred2 = it.next();
		}
		else{
			throw new IllegalArgumentException("Not enough predicates in query.");
		}

		IEJoinInMemory join = IEJoinInMemory.fromPred(pred1, pred2, _allProjRels);
		join.getResult();

		while(it.hasNext()){
			pred = it.next();
		}
	}

	private void _populateProjRels(Query query) {
		_allProjRels = new HashMap<String, Set<Integer>>();

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
			_allProjRels.put(rel.table, new HashSet<Integer>());
		}

		_allProjRels.get(rel.table).add(rel.col);
	}
}
