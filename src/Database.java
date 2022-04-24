/**
 * A class for the Database object.  It's contains the core main loop that makes up a database.
 * <p>
 * The program will be ran as:
 * java Database <db_loc> <page_size> <buffer_size>
 * <p>
 * authors: Scott Johnson, Kyle Ferguson
 */

import catalog.ACatalog;
import catalog.Catalog;
import common.Utilities;
import parsers.DDLParser;
import parsers.DMLParser;
import parsers.ResultSet;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;
import filesystem.FileSystem;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Scanner;

/*
    This is the main driver class for the database.

    It is responsible for starting and running the database.

    Other than in the provided testers this will be the only class to contain a main.

    You will add functionality to this class during the different phases.

    More details in the writeup.
 */
public class Database {

    /**
     * This function will be used to start/restart a database. It will:
     *  -create/restore the catalog.
     *  -create/restore the storage manger.
     * It will then go into a loop asking for SQL statements/queries. It will call the proper parsing
     * function based on statement or query. It will process multiple input lines as a single state-
     * ment/query until it gets a semi-colon;
     *
     * To exit the program the command quit; will be entered.
     * After each non-quit statement it will report ERROR or SUCCESS based on the results of the statement.
     * Queries will display the results of the query in an easily readable/understandable form.
     * (Later phase)
     *
     * @param args DB location, Page Size, Buffer Size
     */
    public static void main(String[] args) {

        // Startup time!  Establish a catalog and a storage manager for us to work with
        Catalog.createCatalog(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        StorageManager.createStorageManager();

        System.out.println("""
                Welcome!  Begin by entering some DDL statements, then feel free to query using DML statements. \s
                All statements should end with a ";".
                Exit using the statement "quit;".""");
        Scanner scn = new Scanner(System.in);
        String statement = "";
        while (true) {
            System.out.println("Enter Statement:");
            String input;
//create table foo( baz double primarykey );insert into foo values (21.2), (1000000.2), (0.0000001);
            // create table test( attr1 double primarykey, attr2 varchar(5) );
            do {
                input = scn.next();
                statement += input + " ";
            }
            while (!input.contains(";"));
            System.out.println(statement.toLowerCase());

            if (statement.strip().equals("quit;")) {  // end of entering statements
                System.out.println("SUCCESS, Exiting Now...");
                break;
            } else if (statement.strip().toLowerCase().startsWith("select")) {
                //TODO kyle is this right to put this here like this??
                ResultSet tempset = null;
                try {
                    tempset = executeQuery(statement);
                }
                catch (Error e){
                    System.err.println("ERROR: There was an error executing the query.");
                }
                if(tempset != null) {
                    try {
                        Utilities.prettyPrintResultSet(tempset, false, 16);
                    }
                    catch(Error e){
                        System.err.println("ERROR: There was an error printing the query.");
                    }
                }
                else
                    System.err.println("ERROR");
            }
            else{
                try {
                    if (executeStatement(statement))
                        System.out.println("SUCCESS");
                    else
                        System.err.println("ERROR");
                }
                catch (Error e){
                    System.err.println("ERROR");
                }
            }


            statement = "";  // reset the statement to be blank
        }

        terminateDatabase();
    }

    /**
     * This function will be used when executing database statements that do not return anything.
     * For instance:
     * -schema creation/modification
     * -insert
     * -delete
     * -update
     * <p>
     * Determines the different types and sends them to the proper parser; DDL or DML.
     * @param stmt the statement to be executed
     * @return True if successful, False otherwise
     */
    public static boolean executeStatement(String stmt) {
        if (DDLParser.parseDDLStatement(stmt)) {
            return true;
        } else {
            return DMLParser.parseDMLStatement(stmt);
        }
    }

    /**
     * This function will be used when executing database queries that return tables of data.
     *
     * @param queryStmt The query to be executed
     * @return The table of data
     */
    public static ResultSet executeQuery(String queryStmt) {
        ResultSet QueryResultTable = DMLParser.parseDMLQuery(queryStmt);
        if (QueryResultTable == null) {
            System.err.println("query: " + queryStmt + " failed");
            return null;
        }
        return QueryResultTable;
    }

    /**
     * This function will be used to safely shutdown the database.
     * Stores any needed data needed to restart the database to physical hardware.
     *
     * @return True if successful, False otherwise
     */
    public static boolean terminateDatabase() {

        // purge sm buffer so that any changes in buffer are committed
        StorageManager.getStorageManager().purgePageBuffer();

        // save catalog to disk
        Catalog.getCatalog().saveToDisk();

        return false;
    }
}
