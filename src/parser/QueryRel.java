package parser;

public class QueryRel {
	public String table;
	public int col;
	
	public QueryRel(String table, int col){
		this.col = col;
		this.table = table;
	}
	
	@Override
	public boolean equals(Object o){
		QueryRel other = (QueryRel) o;
		
		return other.col == this.col && other.table.equals(this.table);
	}
	
	@Override
	public String toString(){
		return table + "." + col;
	}
}
