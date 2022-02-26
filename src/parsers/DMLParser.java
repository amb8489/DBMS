package parsers;

import catalog.Catalog;
import common.ITable;
import common.Table;
import common.VerbosePrint;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.List;

/*
  Class for DML parser

  This class is responsible for parsing DDL statements

  You will implement the parseDMLStatement and parseDMLQuery functions.
  You can add helper functions as needed, but the must be private and static.

  @author Scott C Johnson (sxjcs@rit.edu)

 */
public class DMLParser {

    /**
     * This function will parse and execute DML statements (insert, delete, update, etc)
     * <p>
     * This will be used for parsing DML statement that do not return data
     *
     * @param stmt the statement to parse/execute
     * @return true if successfully parsed/executed; false otherwise
     */
    public static boolean parseDMLStatement(String stmt) {

        if (stmt.toUpperCase().startsWith("DELETE")) {
            deleteFromTable(stmt);
        }
        return true;
    }

    // delete from <name> where <condition>
    private static void deleteFromTable(String stmt) {


        try {


            // removes redundant spaces and new lines
            stmt = stmt.replace(";","");
            List<String> tokens = StringFormatter.mkTokensFromStr(stmt);
            System.out.println(tokens);

            String tableName = tokens.get(2);

            System.out.println("deleting from table name:" + tableName);

            // cehck table exists
            ITable table = Catalog.getCatalog().getTable(tableName);

            boolean removeEverything = tokens.size() == 3;
            if (removeEverything) {
                VerbosePrint.print("removing everyting from table : " + tableName);
                ((StorageManager) StorageManager.getStorageManager()).deleteRecordWhere(table, "", removeEverything);
                return;
            }


            //WHERE CLAUSE
            String Where = String.join(" ", tokens.subList(4, tokens.size())).replace(";", "");
            System.out.println("where{" + Where + "}");
            // deleteing where
            ((StorageManager) StorageManager.getStorageManager()).deleteRecordWhere(table, Where, removeEverything);


        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("error in removing in DML");
        }


    }

    /**
     * This function will parse and execute DML statements (select)
     * <p>
     * This will be used for parsing DML statement that return data
     *
     * @param query the query to parse/execute
     * @return the data resulting from the query; null upon error.
     * Note: No data and error are two different cases.
     */
    public static ResultSet parseDMLQuery(String query) {
        return null;
    }


    public static void main(String[] args) {

        DMLParser.parseDMLStatement("delete    from   classList;");
    }
}
