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

	public int runQuery() throws JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		IEJoinEstimator join = new IEJoinEstimator(_r1, _r2, _r1c1, _r2c1, _r1c2,
				_r2c2, _op1, _op2, _projRels,_table1Size,_table2Size);
		return join.getResult();
	}
}

class IEJoinInMemorySelectivityEstimate implements GlobalConst {
	private static IEJoinInMemoryQuerySelectivityEstimator _query;
	private static int[] _sizeOfTables;

	private IEJoinInMemorySelectivityEstimate() {
	}

	public static int[] queryFromFile(String filename) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		return parseAndRunQuery(filename);
	}

	public static int[] query2c_estimate() throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception {
		return queryFromFile("queries/phase4_query.txt");
	}

	public static boolean runTests(int[] sizeOfTables) {
		try {
			_sizeOfTables = sizeOfTables;
			int[] result = query2c_estimate();
			
			//TODO
			for(int i=0;i<result.length;i++){
				System.out.println(result[i]);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	private static int[] parseAndRunQuery(String filename) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		File file = new File(filename);

		if (!file.exists()) {
			System.out.println("File at '" + filename + "' not found.");
			return new int[0];
		}

		String line, r1, r2;
		boolean twoPred = false;
		int r1c1 = -1, r1c2 = -1, r2c1 = -1, r2c2 = -1, op1 = -1, op2 = -1;
		String[] parts, relParts,tableNames;
		Scanner scan;
		Map<String, Set<Integer>> projRels = new LinkedHashMap<String, Set<Integer>>();
		int resultCounter = 0;

		
		//Finding total number of conditions
		Scanner scanner2 = new Scanner(file);
		scanner2.nextLine();
		scanner2.nextLine();
		int totalNumberOfconditions = 0;
		while(scanner2.hasNext()){
			totalNumberOfconditions++;
			scanner2.nextLine();
		}
		
		int result[] = new int[totalNumberOfconditions];
		
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

				
				//Getting the table size
				int tableSizeOne = 0, tableSizeTwo =0;
				
				if(tableNames[i].equalsIgnoreCase("f1")){
					tableSizeOne = _sizeOfTables[0];
				}
				else if(tableNames[i].equalsIgnoreCase("f2")){
					tableSizeOne = _sizeOfTables[1];
				}
				else if(tableNames[i].equalsIgnoreCase("f3")){
					tableSizeOne = _sizeOfTables[2];
				}
				else if(tableNames[i].equalsIgnoreCase("f4")){
					tableSizeOne = _sizeOfTables[3];
				}
				else if(tableNames[i].equalsIgnoreCase("f5")){
					tableSizeOne = _sizeOfTables[4];
				}
				
				
				if(tableNames[i+1].equalsIgnoreCase("f1")){
					tableSizeTwo = _sizeOfTables[0];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f2")){
					tableSizeTwo = _sizeOfTables[1];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f3")){
					tableSizeTwo = _sizeOfTables[2];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f4")){
					tableSizeTwo = _sizeOfTables[3];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f5")){
					tableSizeTwo = _sizeOfTables[4];
				}
				

				_query = new IEJoinInMemoryQuerySelectivityEstimator(
						tableNames[i], tableNames[i + 1], r1c1, r2c1, r1c2,
						r2c2, op1, op2, projRels,tableSizeOne,tableSizeTwo);
				
				result[resultCounter] = _query.runQuery();
				resultCounter++;
			}

			scan.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return result;
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

		int[] sizeOfTables = DBBuilderP4.build();
		
		boolean sortstatus = IEJoinInMemorySelectivityEstimate.runTests(sizeOfTables);

	}
}
