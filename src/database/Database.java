/**
 * A class for the Database object.  It's contains the core main loop that makes up a database.
 *
 * The program will be ran as:
 *  java Database <db_loc> <page_size> <buffer_size>
 *
 * authors: Scott Johnson, Kyle Ferguson
 */

package database;

import catalog.ACatalog;
import catalog.Catalog;
import parsers.DDLParser;
import parsers.DMLParser;
import parsers.ResultSet;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;
import filesystem.FileSystem;

import java.util.ArrayList;
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
     * @param args DB location, Page Size, Buffer Size
     */
    public static void main(String[] args) {

        // Startup time!  Establish a catalog and a storage manager for us to work with
        Catalog.createCatalog(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        StorageManager.createStorageManager();

        Scanner scn = new Scanner(System.in);
        String statement = "";
        while(true){
            String input;

            do{
                input = scn.next();
                statement += "\n" + input;
            }
            while(!input.contains(";"));

            if(statement.strip().equals("quit;")){  // end of entering statements
                System.out.println("SUCCESS, Exiting Now...");
                break;
            }
            if (statement.toLowerCase().startsWith("select")){
                //TODO kyle is this right to put this here like this??
                executeQuery(statement);

            }else if(executeStatement(statement)){
                System.out.println("SUCCESS");
            } else{
                System.err.println("ERROR");
            }

            statement = "";  // reset the statement to be blank
        }

        terminateDatabase();
    }

    /**
     * This function will be used when executing database statements that do not return anything.
     * For instance:
     *  -schema creation/modification
     *  -insert
     *  -delete
     *  -update
     *
     * Determines the different types and sends them to the proper parser; DDL or DML.
     * @param stmt the statement to be executed
     * @return True if successful, False otherwise
     */
    public static boolean executeStatement(String stmt){
        if(DDLParser.parseDDLStatement(stmt)){
            System.out.println("ddl");
            return true;
        }
        else{
            return DMLParser.parseDMLStatement(stmt);
        }
    }

    /**
     * //TODO To be completed in a later phase
     * This function will be used when executing database queries that return tables of data.
     *
     * @param queryStmt The query to be executed
     * @return The table of data
     */
    public static ResultSet executeQuery(String queryStmt){
        ResultSet QueryResultTable = DMLParser.parseDMLQuery(queryStmt);
        if(QueryResultTable == null){
            System.err.println("query: "+ queryStmt +"failed");
            return null;
        }

        // TODO print the table


        return QueryResultTable;
    }

    /**
     * This function will be used to safely shutdown the database.
     * Stores any needed data needed to restart the database to physical hardware.
     * @return True if successful, False otherwise
     */
    public static boolean terminateDatabase(){

        // purge sm buffer so that any changes in buffer are committed
        StorageManager.getStorageManager().purgePageBuffer();

        // save catalog to disk
        Catalog.getCatalog().saveToDisk();

        return false;
    }
}
