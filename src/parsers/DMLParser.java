package parsers;

import java.util.ArrayList;

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
     *
     * This will be used for parsing DML statement that do not return data
     *
     * @param stmt the statement to parse/execute
     * @return true if successfully parsed/executed; false otherwise
     */
    public static boolean parseDMLStatement(String stmt){
        return true;
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
}
