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

    private static Object convertAttributeType (String attributeType, String attribute) {
        switch (attributeType) {
            case "Integer":
                return Integer.parseInt(attribute);
            case "Double":
                return Double.parseDouble(attribute);
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

            // check table exists
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
                            .getAttributeType(), tokens.get(i).substring(1)));
                    i++;
                    while (!tokens.get(i).endsWith(")")) {
                        record.add(convertAttributeType(attributes.get(i-(4 + (attributes.size() * numberOfInserts)))
                                .getAttributeType(), tokens.get(i)));
                        i++;
                    }
                    record.add(convertAttributeType(attributes.get(i-(4 + (attributes.size() * numberOfInserts)))
                            .getAttributeType(), tokens.get(i).substring(0, tokens.get(i).length()-1)));

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

            // check table exists
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
                                if (attributes.get(i).getAttributeName().equals(tokens.get(6))) {
                                    newRow.add(convertAttributeType(attributes.get(i).getAttributeType(), tokens.get(5)));
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

//    // delete from <name> where <condition>
//    private static void deleteFromTable(String stmt) {
//
//
//        try {
//
//
//            // removes redundant spaces and new lines
//            stmt = stmt.replace(";","");
//            List<String> tokens = StringFormatter.mkTokensFromStr(stmt);
//            System.out.println(tokens);
//
//            String tableName = tokens.get(2);
//
//            System.out.println("deleting from table name:" + tableName);
//
//            // cehck table exists
//            ITable table = Catalog.getCatalog().getTable(tableName);
//
//            boolean removeEverything = tokens.size() == 3;
//            if (removeEverything) {
//                VerbosePrint.print("removing everyting from table : " + tableName);
//                ((StorageManager) StorageManager.getStorageManager()).deleteRecordWhere(table, "", removeEverything);
//                return;
//            }
//            //WHERE CLAUSE
//            String Where = String.join(" ", tokens.subList(4, tokens.size())).replace(";", "");
//            System.out.println("where{" + Where + "}");
//            // deleteing where
//            ((StorageManager) StorageManager.getStorageManager()).deleteRecordWhere(table, Where, removeEverything);
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.err.println("error in removing in DML");
//        }
//
//
//    }

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
        DMLParser.parseDMLStatement("insert into goalScorers \n values \n (1, \"Karim Benzema\", 3)," +
                "(2, \"Kylian Mbappe\", 1)");
        DMLParser.parseDMLStatement("insert into goalScorers \n values \n (1, \"Thomas Muller\", 1)");
        // test update
        DMLParser.parseDMLStatement("update goalScorers \n set Name = \"Robert Lewandowski\" \n where ID = 1");
    }
}
