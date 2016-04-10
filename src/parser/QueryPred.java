package parser;

import global.AttrOperator;

public class QueryPred {
	public QueryRel leftRel, rightRel;
	public AttrOperator op;
	
	public QueryPred(QueryRel leftRel, AttrOperator op, QueryRel rightRel){
		this.leftRel = leftRel;
		this.op = op;
		this.rightRel = rightRel;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		
		sb.append(leftRel);
		sb.append(" " + _opToString() + " ");
		sb.append(rightRel);
		
		return sb.toString();
	}
	
	private String _opToString(){
		String str;
		
		switch(op.attrOperator){
		case AttrOperator.aopLT:
			str = "<";
			break;
		case AttrOperator.aopLE:
			str = "<=";
			break;
		case AttrOperator.aopGE:
			str = ">=";
			break;
		case AttrOperator.aopGT:
			str = ">";
			break;
		default:
			throw new IllegalArgumentException(Integer.toString(op.attrOperator));
		}

		return str;
	}
}
