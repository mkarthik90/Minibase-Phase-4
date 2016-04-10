package parser;

public class QueryRel {
	public String col, table;
	
	public QueryRel(String table, String col){
		this.col = col;
		this.table = table;
	}
	
	@Override
	public boolean equals(Object o){
		QueryRel other = (QueryRel) o;
		
		return other.col.equals(this.col) && other.table.equals(this.table);
	}
	
	@Override
	public String toString(){
		return table + "." + col;
	}
}
