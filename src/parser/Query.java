package parser;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import global.AttrOperator;

public class Query {
	public Set<QueryRel> select;
	public Set<String> from;
	public Set<QueryPred> where;
	public boolean count;

	public Query(){
		select = new HashSet<QueryRel>();
		from = new HashSet<String>();
		where = new LinkedHashSet<QueryPred>();
		count = false;
	}

	public void setCount(){
		count = true;
	}

	public void addSelect(String table, int col){
		if(!count){
			select.add(new QueryRel(table, col));
		}
		else{
			throw new IllegalArgumentException("Count is set.");
		}
	}

	public void addFrom(String str){
		from.add(str);
	}

	public void addWhere(String rel1, AttrOperator op, String rel2){
		String t1, t2;
		int c1, c2;

		t1 = rel1.split("_")[0];
		c1 = Integer.parseInt(rel1.split("_")[1]);
		t2 = rel2.split("_")[0];
		c2 = Integer.parseInt(rel2.split("_")[1]);

		addWhere(new QueryRel(t1, c1), op, new QueryRel(t2, c2));
	}

	public void addWhere(QueryRel r1, AttrOperator op, QueryRel r2){
		where.add(new QueryPred(r1, op, r2));
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();

		sb.append("SELECT");

		if(count){
			sb.append(" count(*)");
		}
		else{
			for(QueryRel rel : select){
				sb.append(" " + rel);
			}
		}

		sb.append("\nFROM");

		for(String str : from){
			sb.append(" " + str);
		}

		sb.append("\nWHERE\n");

		QueryPred pred;
		Iterator<QueryPred> predIt = where.iterator();

		while(predIt.hasNext()){
			pred = predIt.next();

			sb.append(pred);

			if(predIt.hasNext()){
				sb.append("\nAND\n");
			}
		}

		return sb.toString();
	}
}
