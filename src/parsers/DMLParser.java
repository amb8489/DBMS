package parsers;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.ITable;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.List;

/*
  Class for DML parser

  This class is responsible for parsing DDL statements

  You will implement the parseDMLStatement and parseDMLQuery functions.
  You can add helper functions as needed, but the must be private and static.

  @author Scott C Johnson (sxjcs@rit.edu)
  @author Aryan Jha (axj2613@rit.edu)

 */
public class DMLParser {

    /**
     * This function will parse and execute DML statements (insert, delete, update, etc)
     *
     * This will be used for parsing DML statement that do not return data
     *
     * @param stmt the statement to parse/execute
     * @return true if successfully parsed/executed; false otherwise
     */
    public static boolean parseDMLStatement(String stmt){
        if (stmt.toUpperCase().startsWith("INSERT")) {
            insertTable(stmt);
        }
        if (stmt.toUpperCase().startsWith("UPDATE")) {
            updateTable(stmt);
        }
        return true;
    }

    /**
     * This function evaluates the mathematical result of a given 'set' expression of an update statement
     * @param attributeType
     * @param statement1 statement to the left of the operator
     * @param operator math operator
     * @param statement2 statement to the right of the operator
     * @param attributesList
     * @param row the row from the table that meets the 'where' condition
     * @return
     */
    private static Object evalSetMath (String attributeType, String statement1, String operator, String statement2,
                                       ArrayList<Attribute> attributesList, ArrayList<Object> row) {
        switch (attributeType) {
            case "Integer":
                int attr1Int = 0;
                boolean attr1IntChanged = false;
                int attr2Int = 0;
                boolean attr2IntChanged = false;
                // check if the statements are placeholder variables
                for (int i = 0; i < attributesList.size(); i++) {
                    if (attributesList.get(i).getAttributeName().equals(statement1)) {
                        attr1Int = (int) row.get(i);
                        attr1IntChanged = true;
                    }
                    if (attributesList.get(i).getAttributeName().equals(statement2)) {
                        attr2Int = (int) row.get(i);
                        attr2IntChanged = true;
                    }
                }
                // if the statements are not placeholder variables, set them as literal int values
                if (!attr1IntChanged) {
                    attr1Int = Integer.parseInt(statement1);
                }
                if (!attr2IntChanged) {
                    attr2Int = Integer.parseInt(statement2);
                }
                // perform math on the statements
                switch (operator) {
                    case "+":
                        return attr1Int + attr2Int;
                    case "-":
                        return attr1Int - attr2Int;
                    case "*":
                        return attr1Int * attr2Int;
                    case "/":
                        return attr1Int / attr2Int;
                }
            case "Double":
                double attr1Double = 0;
                boolean attr1DoubleChanged = false;
                double attr2Double = 0;
                boolean attr2DoubleChanged = false;
                // check if the statements are placeholder variables
                for (int i = 0; i < attributesList.size(); i++) {
                    if (attributesList.get(i).getAttributeName().equals(statement1)) {
                        attr1Double = (double) row.get(i);
                        attr1DoubleChanged = true;
                    }
                    if (attributesList.get(i).getAttributeName().equals(statement2)) {
                        attr2Double = (double) row.get(i);
                        attr2DoubleChanged = true;
                    }
                }
                // if the statements are not placeholder variables, set them as literal double values
                if (!attr1DoubleChanged) {
                    attr1Double = Double.parseDouble(statement1);
                }
                if (!attr2DoubleChanged) {
                    attr2Double = Double.parseDouble(statement2);
                }
                // perform math on the statements
                switch (operator) {
                    case "+":
                        return attr1Double + attr2Double;
                    case "-":
                        return attr1Double - attr2Double;
                    case "*":
                        return attr1Double * attr2Double;
                    case "/":
                        return attr1Double / attr2Double;
                }
            default:
                return null;
        }
    }

    /**
     * This function converts an attribute to the given attributeType. The attribute given can be a mathematical expression.
     * @param attributeType
     * @param attribute
     * @param attributesList
     * @param row the row from the table that meets the 'where' condition
     * @return
     */
    private static Object convertAttributeType (String attributeType, String attribute, ArrayList<Attribute> attributesList,
                                                ArrayList<Object> row) {
        String[] expression = attribute.split(" ");
        switch (attributeType) {
            case "Integer":
                if (expression.length == 1) {
                    return Integer.parseInt(attribute);
                }
                else {
                    return evalSetMath(attributeType, expression[0], expression[1], expression[2], attributesList, row);
                }
            case "Double":
                if (expression.length == 1) {
                    return Double.parseDouble(attribute);
                }
                else {
                    return evalSetMath(attributeType, expression[0], expression[1], expression[2], attributesList, row);
                }
            case "Boolean":
                return Boolean.parseBoolean(attribute);
            default:
                return attribute;
        }
    }

    // insert into <name> values <tuples>
    private static void insertTable(String stmt) {
        try{
            // removes redundant spaces and new lines
            stmt = stmt.replace(","," ");
            stmt = stmt.replace(";","");
            List<String> tokens = StringFormatter.mkTokensFromStr(stmt);
            System.out.println(tokens);

            String tableName = tokens.get(2);

            // check if table exists; get table
            if (!Catalog.getCatalog().containsTable(tableName)) {
                System.err.println("The catalog does not contain the table: " + tableName);
                return;
            }

            System.out.println("inserting to table: " + tableName);

            ITable table = Catalog.getCatalog().getTable(tableName);
            ArrayList<Attribute> attributes = table.getAttributes();

            int numberOfInserts = 0;
            if (tokens.get(3).equalsIgnoreCase("values")) {
                for (int i = 4; i < tokens.size(); i++) {
                    ArrayList<Object> record = new ArrayList<>();
                    if (!tokens.get(i).startsWith("(")) {
                        System.err.println("The tuples in the insert statement are not in the correct format; " +
                                "misplaced/missing opening parenthesis");
                        return;
                    }

                    record.add(convertAttributeType(attributes.get(i-(4 + (attributes.size() * numberOfInserts)))
                            .getAttributeType(), tokens.get(i).substring(1), null, null));
                    i++;
                    while (!tokens.get(i).endsWith(")")) {
                        record.add(convertAttributeType(attributes.get(i-(4 + (attributes.size() * numberOfInserts)))
                                .getAttributeType(), tokens.get(i), null, null));
                        i++;
                    }
                    record.add(convertAttributeType(attributes.get(i-(4 + (attributes.size() * numberOfInserts)))
                            .getAttributeType(), tokens.get(i).substring(0, tokens.get(i).length()-1), null, null));

                    System.out.println(record);

                    if (record.size() == attributes.size()) {
                        boolean insertSuccess = StorageManager.getStorageManager().insertRecord(table, record);
                        System.out.println("insert success: " + insertSuccess);
                    }
                    else {
                        System.err.println("The tuples in the insert statement are not in the correct format; " +
                                "incorrect number of attributes");
                        return;
                    }
                    numberOfInserts++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("error in inserting using DML");
        }
    }

    // update <name> set <column_1> = value where <condition>;
    private static void updateTable(String stmt) {
        try {
            // removes redundant spaces and new lines
            stmt = stmt.replace(";","");
            List<String> tokens = StringFormatter.mkTokensFromStr(stmt);
            System.out.println(tokens);

            String tableName = tokens.get(1);

            // check if table exists; get table
            if (!Catalog.getCatalog().containsTable(tableName)) {
                System.err.println("The catalog does not contain the table: " + tableName);
                return;
            }

            System.out.println("updating the table: " + tableName);

            ITable table = Catalog.getCatalog().getTable(tableName);

            ArrayList<ArrayList<Object>> records = StorageManager.getStorageManager().getRecords(table);
            ArrayList<Attribute> attributes = table.getAttributes();

            if (tokens.get(6).equalsIgnoreCase("where")) {
                WhereParser wp = new WhereParser();
                for (ArrayList<Object> row: records) {
                    if (wp.whereIsTrue(stmt, row, attributes)) {
                        if (tokens.get(2).equalsIgnoreCase("set")) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            for (int i = 0; i < attributes.size(); i++) {
                                if (attributes.get(i).getAttributeName().equals(tokens.get(3))) {
                                    newRow.add(convertAttributeType(attributes.get(i).getAttributeType(), tokens.get(5), attributes, row));
                                }
                                else {
                                    newRow.add(row.get(i));
                                }
                            }
                            boolean updateSuccess = StorageManager.getStorageManager().updateRecord(table, row, newRow);
                            System.out.println("update success:" + updateSuccess);
                        }
                    }
                }
            }
            else if (tokens.size() > 10 && tokens.get(8).equalsIgnoreCase("where")) {
                WhereParser wp = new WhereParser();
                for (ArrayList<Object> row: records) {
                    if (wp.whereIsTrue(stmt, row, attributes)) {
                        if (tokens.get(2).equalsIgnoreCase("set")) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            for (int i = 0; i < attributes.size(); i++) {
                                if (attributes.get(i).getAttributeName().equals(tokens.get(3))) {
                                    newRow.add(convertAttributeType(attributes.get(i).getAttributeType(), tokens.get(5)
                                            + " " + tokens.get(6) + " " + tokens.get(7), attributes, row));
                                }
                                else {
                                    newRow.add(row.get(i));
                                }
                            }
                            boolean updateSuccess = StorageManager.getStorageManager().updateRecord(table, row, newRow);
                            System.out.println("update success:" + updateSuccess);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("error in updating using DML");
        }
    }


    /**
     * This function will parse and execute DML statements (select)
     *
     * This will be used for parsing DML statement that return data
     * @param query the query to parse/execute
     * @return the data resulting from the query; null upon error.
     *         Note: No data and error are two different cases.
     */
    public static ResultSet parseDMLQuery(String query){
        return null;
    }


    public static void main(String[] args) {
        ACatalog catalog = ACatalog.createCatalog("/Users/aryanjha/Documents/CSCI 421", 100, 10);
        AStorageManager sm = AStorageManager.createStorageManager();

        ArrayList<Attribute> attributes = new ArrayList<>();

        attributes.add(new Attribute("ID", "Integer"));
        attributes.add(new Attribute("Name", "VarChar(20)"));
        attributes.add(new Attribute("Goals", "Integer"));

        Attribute pk = attributes.get(0);

        ITable table1 = catalog.addTable("goalScorers", attributes, pk);

        // test insert
        DMLParser.parseDMLStatement("insert into goalScorers \n values \n (1, \"Karim Benzema\", 7)," +
                "(2, \"Kylian Mbappe\", 1)");
        DMLParser.parseDMLStatement("insert into goalScorers \n values \n (1, \"Thomas Muller\", 1)");
        // test update
        System.out.println("Before update call: " + sm.getRecords(table1));
        DMLParser.parseDMLStatement("update goalScorers \n set Goals = ID + 2 \n where ID = 1");
        System.out.println("After update call: " + sm.getRecords(table1));
    }
}
