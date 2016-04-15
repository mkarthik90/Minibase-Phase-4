package tests;

//originally from : joins.C

import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.IEJoinEstimator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import bufmgr.PageNotReadException;

class IEJoinInMemoryQuerySelectivityEstimator {
	private static String _r1, _r2;
	private static Map<String, Set<Integer>> _projRels;
	private static int _r1c1, _r1c2, _r2c1, _r2c2, _op1, _op2;
	private static AttrType[] _attrTypes;
	private static int _table1Size = 0, _table2Size = 0;

	public IEJoinInMemoryQuerySelectivityEstimator(String r1, String r2,
			int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2,
			Map<String, Set<Integer>> projRels, int table1Size, int table2Size) {
		_r1 = r1;
		_r2 = r2;
		_r1c1 = r1c1;
		_r1c2 = r1c2;
		_r2c1 = r2c1;
		_r2c2 = r2c2;
		_op1 = op1;
		_op2 = op2;
		_projRels = projRels;
		_table1Size = table1Size;
		_table2Size = table2Size;

		int tempSize = 0;

		for (String key : projRels.keySet()) {
			tempSize += projRels.get(key).size();
		}

		_attrTypes = new AttrType[tempSize];

		for (int i = 0; i < _attrTypes.length; i++) {
			_attrTypes[i] = new AttrType(AttrType.attrInteger);
		}
	}

	public void runQuery() throws JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		IEJoinEstimator join = new IEJoinEstimator(_r1, _r2, _r1c1, _r2c1, _r1c2,
				_r2c2, _op1, _op2, _projRels,0,0);
		join.getResult();
	}
}

class IEJoinInMemorySelectivityEstimate implements GlobalConst {
	private static IEJoinInMemoryQuerySelectivityEstimator _query;

	private IEJoinInMemorySelectivityEstimate() {
	}

	public static void queryFromFile(String filename) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		parseQuery(filename);
	}

	public static void query2c() throws JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		queryFromFile("queries/query_2c.txt");
	}

	public static void query2c_1() throws JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		queryFromFile("queries/p3/query_2c_1.txt");
	}

	public static void query2c_estimate() throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		queryFromFile("queries/p3/query_2c_2.txt");
	}

	public static boolean runTests() {
		try {
			query2c_estimate();
		//	query2c_1();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	private static boolean parseQuery(String filename) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		File file = new File(filename);

		if (!file.exists()) {
			System.out.println("File at '" + filename + "' not found.");
			return false;
		}

		String line, r1, r2;
		boolean twoPred = false;
		int r1c1 = -1, r1c2 = -1, r2c1 = -1, r2c2 = -1, op1 = -1, op2 = -1;
		String[] parts, relParts,tableNames;
		Scanner scan;
		Map<String, Set<Integer>> projRels = new LinkedHashMap<String, Set<Integer>>();

		try {
			scan = new Scanner(file);

			parts = scan.nextLine().trim().split(" ");

			// Select Clause
			for (String part : parts) {
				relParts = part.split("_");

				if (!projRels.containsKey(relParts[0])) {
					projRels.put(relParts[0], new LinkedHashSet<Integer>());
				}

				projRels.get(relParts[0]).add(Integer.parseInt(relParts[1]));
			}

			// From Clause
			//R S T V W
			tableNames = scan.nextLine().trim().split(" ");
			
//			r1 = parts[0];
//
//			if (parts.length > 1) {
//				r2 = parts[1];
//			} else {
//				r2 = r1;
//			}

			
			
			// This loop will run for each two table join. If there are 4 tables
			// to join with 8 predicates, this loop will run for 4 times to
			// calculate 4 different estimate
			for (int i = 0; i < tableNames.length; i = i + 2) {

				// Where Clause

				// while (scan.hasNextLine()) {
				twoPred = false;
				int totalNumberOfCondition = 2;
				while (totalNumberOfCondition > 0) {
					line = scan.nextLine().trim();

					if (line.equals("AND")) {
						twoPred = true;
					} else {
						parts = line.split(" ");

						if (twoPred) {
							relParts = parts[0].split("_");
							r1c2 = Integer.parseInt(relParts[1]);

							op2 = Integer.parseInt(parts[1]);

							relParts = parts[2].split("_");
							r2c2 = Integer.parseInt(relParts[1]);
						} else {
							relParts = parts[0].split("_");
							r1 = relParts[0];
							r1c1 = Integer.parseInt(relParts[1]);

							op1 = Integer.parseInt(parts[1]);

							relParts = parts[2].split("_");
							r2 = relParts[0];
							r2c1 = Integer.parseInt(relParts[1]);
						}
					}
					totalNumberOfCondition--;
				}
				// }

				if (!twoPred) {
					op2 = op1;
					r1c2 = r1c1;
					r2c2 = r2c1;
				}

				op1 = getAttrOp(op1);
				op2 = getAttrOp(op2);

				if (op1 == -1 || op2 == -1) {
					scan.close();
					return false;
				}

				_query = new IEJoinInMemoryQuerySelectivityEstimator(
						tableNames[i], tableNames[i + 1], r1c1, r2c1, r1c2,
						r2c2, op1, op2, projRels,0,0);
				
				_query.runQuery();
			}

			scan.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return true;
	}

	private static int getAttrOp(int op) {
		int ret;

		switch (op) {
		case 1:
			ret = AttrOperator.aopLT;
			break;
		case 2:
			ret = AttrOperator.aopLE;
			break;
		case 3:
			ret = AttrOperator.aopGE;
			break;
		case 4:
			ret = AttrOperator.aopGT;
			break;
		default:
			ret = -1;
			break;
		}

		return ret;
	}
}

public class SelectivityEstimatorTest {
	public static void main(String argv[]) {

		DBBuilderP4.build();
		
		Map<String, Set<Integer>> projRels = new LinkedHashMap<String, Set<Integer>>();
		projRels.put("R", new LinkedHashSet<Integer>());
		projRels.get("R").add(1);

		projRels.put("S", new LinkedHashSet<Integer>());
		projRels.get("S").add(1);

		boolean sortstatus = IEJoinInMemorySelectivityEstimate.runTests();

	}
}
