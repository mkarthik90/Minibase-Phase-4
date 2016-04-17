package tests;

//originally from : joins.C

import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.IEJoinEstimator;
import iterator.IEJoinInMemory;
import iterator.IEJoinInMemoryP4;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import parser.Query;
import parser.QueryPred;
import parser.QueryRel;
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
	private static IEJoinEstimator _query;
	private static int[] _sizeOfTables;
	private static List<List<QueryPred>> queryPredicateList = new LinkedList<List<QueryPred>>();

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
			int minimumValue =0, minimumValuePosition = 0;
			//TODO
			for(int i=0;i<result.length;i++){
				
				if(i == 0){
					minimumValue = result[i];
					minimumValuePosition = i;
				}
				
				if(minimumValue > result[i]){
					minimumValue = result[i];
					minimumValuePosition = i;
				}
			}
			
			System.out.println("Minimym valuye is in position"+minimumValuePosition);
			System.out.println("Minimum value is "+minimumValue);
			
			Query q = new Query();

			for (int i = minimumValuePosition; i < result.length; i++) {
				List<QueryPred> queryPredicates = queryPredicateList.get(i);
				q.addWhere(queryPredicates.get(0));
				q.addWhere(queryPredicates.get(1));
			}

			for (int i = minimumValue - 1; i >= 0; i--) {
				List<QueryPred> queryPredicates = queryPredicateList.get(i);
				q.addWhere(queryPredicates.get(0));
				q.addWhere(queryPredicates.get(1));
			}
			
			IEJoinInMemoryP4 ieJoinP4 = new IEJoinInMemoryP4(q, true);
			
			
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
			if(!scanner2.nextLine().equalsIgnoreCase("and")){
				totalNumberOfconditions++;
			}
		}
		
		int result[] = new int[totalNumberOfconditions/2];
		
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
			boolean firstConditionSet = false;
			
			for (int i = 0; i < tableNames.length - 1; i = i + 1) {

				// Where Clause
				
				if(i != 0){
					//Did this to scan extra AND statement
					scan.nextLine();
				}

				// while (scan.hasNextLine()) {
				int totalNumberOfCondition = 2;
				while (totalNumberOfCondition >= 0) {
					line = scan.nextLine().trim();
					
					

					if (line.equals("AND")) {
						
					} else {
						parts = line.split(" ");

						if (firstConditionSet == false) {
							relParts = parts[0].split("_");
							r1 = relParts[0];
							r1c1 = Integer.parseInt(relParts[1]);

							op1 = Integer.parseInt(parts[1]);

							relParts = parts[2].split("_");
							r2 = relParts[0];
							r2c1 = Integer.parseInt(relParts[1]);
							firstConditionSet = true;
						
						} else {

							relParts = parts[0].split("_");
							r1c2 = Integer.parseInt(relParts[1]);

							op2 = Integer.parseInt(parts[1]);

							relParts = parts[2].split("_");
							r2c2 = Integer.parseInt(relParts[1]);
							firstConditionSet = false;
						}
					}
					totalNumberOfCondition--;
				}
				// }

				op1 = getAttrOp(op1);
				op2 = getAttrOp(op2);

				
				//Getting the table size
				int tableSizeOne = 0, tableSizeTwo =0;
				
				if(tableNames[i].equalsIgnoreCase("f1nr")){
					tableSizeOne = _sizeOfTables[0];
				}
				else if(tableNames[i].equalsIgnoreCase("f2nr")){
					tableSizeOne = _sizeOfTables[1];
				}
				else if(tableNames[i].equalsIgnoreCase("f3nr")){
					tableSizeOne = _sizeOfTables[2];
				}
				else if(tableNames[i].equalsIgnoreCase("f4nr")){
					tableSizeOne = _sizeOfTables[3];
				}
				else if(tableNames[i].equalsIgnoreCase("f5nr")){
					tableSizeOne = _sizeOfTables[4];
				}
				
				
				if(tableNames[i+1].equalsIgnoreCase("f1nr")){
					tableSizeTwo = _sizeOfTables[0];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f2nr")){
					tableSizeTwo = _sizeOfTables[1];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f3nr")){
					tableSizeTwo = _sizeOfTables[2];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f4nr")){
					tableSizeTwo = _sizeOfTables[3];
				}
				else if(tableNames[i+1].equalsIgnoreCase("f5nr")){
					tableSizeTwo = _sizeOfTables[4];
				}
				
				System.out.println("*****");
				System.out.println(tableNames[i]);
				System.out.println(tableNames[i+1]);
				
				
				
				_query = new IEJoinEstimator(
						tableNames[i], tableNames[i + 1], r1c1, r2c1, r1c2,
						r2c2, op1, op2, projRels,tableSizeOne,tableSizeTwo);
				
				QueryRel leftRel1 = new QueryRel(tableNames[i], r1c1);
				QueryRel rightRel1 = new QueryRel(tableNames[i+1], r2c1);
				QueryPred qp  = new QueryPred(leftRel1, new AttrOperator(op1), rightRel1);
				
				
				QueryRel leftRel2 = new QueryRel(tableNames[i], r1c2);
				QueryRel rightRel2 = new QueryRel(tableNames[i+1], r2c2);
				QueryPred qp2  = new QueryPred(leftRel2, new AttrOperator(op2), rightRel2);
				
				
				List<QueryPred> queryPredicate = new LinkedList<QueryPred>();
				
				queryPredicate.add(qp);
				queryPredicate.add(qp2);
				
				queryPredicateList.add(queryPredicate);
				
				result[resultCounter] = _query.getResult();
				System.out.println(result[resultCounter]);
//				/ _query.runQuery();
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
