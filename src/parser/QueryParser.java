package parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import global.AttrOperator;

/*
SELECT count(*)
FROM F1 r, F2 s, F3 t, F4 v, F5 w
WHERE r.salary > s.salary AND r.tax < s.tax
AND s.start < t.end AND s.end > t.start
AND t.salary > v.salary AND t.tax < v.tax
AND v.start < w.end AND v.end > w.start;
 */

public abstract class QueryParser {
	public static Query parse(String filepath) throws FileNotFoundException{
		Query query = new Query();
		File file = new File(filepath);
		Scanner scan = new Scanner(file);
		int lineNo = 0, col;
		AttrOperator op;
		String line, table, leftOp, rightOp;
		String[] spaceParts, _parts;

		while(scan.hasNextLine()){
			line = scan.nextLine().trim();
			spaceParts = line.split(" ");

			if(lineNo == 0){
				if(line.toLowerCase().equals("count")){
					query.setCount();
				}
				else{
					for(String part : spaceParts){
						_parts = part.split("_");

						table = _parts[0];
						col = Integer.parseInt(_parts[1]);

						query.addSelect(table, col);
					}
				}
			}
			else if(lineNo == 1){
				for(String from : spaceParts){
					query.addFrom(from);
				}
			}
			else if(!line.toLowerCase().equals("and")){
				op = _getOP(Integer.parseInt(spaceParts[1]));
				leftOp = spaceParts[0];
				rightOp = spaceParts[2];

				query.addWhere(leftOp, op, rightOp);
			}

			lineNo++;
		}

		scan.close();

		return query;
	}

	private static AttrOperator _getOP(int op){
		AttrOperator ret;

		switch(op){
		case 1:
			ret = new AttrOperator(AttrOperator.aopLT);
			break;
		case 2:
			ret = new AttrOperator(AttrOperator.aopLE);
			break;
		case 3:
			ret = new AttrOperator(AttrOperator.aopGE);
			break;
		case 4:
			ret = new AttrOperator(AttrOperator.aopGT);
			break;
		default:
			throw new IllegalArgumentException(Integer.toString(op));
		}

		return ret;
	}
}
