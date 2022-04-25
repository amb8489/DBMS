package phase2tests;

import catalog.Catalog;
import common.ITable;
import common.Table;
import parsers.DDLParser;
import parsers.DMLParser;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;


public class Phase2Tester {

    private static void printData(ArrayList<ArrayList<Object>> data2) {
        System.out.println("Do you want to print the table entries? (y/n)");
        Scanner scanner = new Scanner(System.in);

        if (scanner.nextLine().equals("y")) {
            System.out.println("Data should be in order by primary key\n");
            if (data2.size() == 0) {
                System.out.println("Table is empty");
                return;
            }
            int rowNum = 1;
            for (ArrayList<Object> row : data2) {
                System.out.printf("Row %d: %s%n", rowNum++, row);
            }
        }
    }

    /**
     * Random string creation
     *
     * @param length the length of the random string
     */
    private static String getSaltString(int length) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        return salt.toString();

    }

    //will create a very large table and insert a lot of data into it
    //No constraints other than pk
    //this tests minimal functionality to create, insert, delete, update, alter, drop
    private static boolean createVeryLargeTable() {

        System.out.println("Testing by creating a very large table...");
        String createTable =
                "create table large( " +
                        "attr1 integer primarykey, " +
                        "attr2 double, " +
                        "attr3 boolean, " +
                        "attr4 char(5), " +
                        "attr5 varchar(10) );";

        System.out.println("Create table stmt:\n" + createTable);

        if (!DDLParser.parseDDLStatement(createTable)) {
            System.err.println("Something went wrong creating the large table");
            return false;
        }

        ITable table = Catalog.getCatalog().getTable("large");
        if (table == null) {
            System.err.println("There was an error getting/creating larger table");
            return false;
        }

        for (int i = 0; i < 100; i++) {
            Random r = new Random();

            String insertStmt = String.format("insert into %s values (%d, %f, %s, %s, %s);",
                    "large", i, r.nextDouble() * 100, r.nextBoolean(),
                    getSaltString(5), getSaltString(Math.abs(r.nextInt()) % 10 + 1));
            if (!DMLParser.parseDMLStatement(insertStmt)) {
                System.err.println("Error inserting data into large table");
                return false;
            }


        }
        ArrayList<ArrayList<Object>> data = StorageManager.getStorageManager().getRecords(table);

        System.out.println("Printing data after insert...");
        printData(data);

        System.out.println("----------------------HERE------------------------");
        ((Table)table).getPkTree().printRPS();


        String deleteStmt = "delete from large where attr1 > 90;";
        System.out.println("Stmt: " + deleteStmt);
        if (!DMLParser.parseDMLStatement(deleteStmt)) {
            System.err.println("Error when deleting from large table");
            return false;
        }

        System.out.println("----------------DELETE--HERE------------------------");
        ((Table)table).getPkTree().printRPS();

        data = StorageManager.getStorageManager().getRecords(table);
        System.out.println("Printing data after delete...");
        printData(data);


        System.out.println("----------------------HERE------------------------");
        ((Table)table).getPkTree().printRPS();


        deleteStmt = "delete from large where attr3 = true or attr2 < 50.0;";
        System.out.println("Stmt: " + deleteStmt);
        if (!DMLParser.parseDMLStatement(deleteStmt)) {
            System.err.println("Error when deleting from large table");
            return false;
        }


        ((Table)table).getPkTree().print();

//        System.exit(999);








        data = StorageManager.getStorageManager().getRecords(table);
        System.out.println("Printing data after delete...");
        printData(data);





        String updateStmt = "update large set attr4 = \"hello\" where attr1 > 70;";
        System.out.println("Stmt: " + updateStmt);
        if (!DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println("Error when updating from large table");
            return false;
        }
        data = StorageManager.getStorageManager().getRecords(table);
        System.out.println("Printing data after update...");
        printData(data);

        updateStmt = "update large set attr2 = attr2 + 2.0 where attr1 > 70;";
        System.out.println("Stmt: " + updateStmt);
        if (!DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println("Error when updating from large table");
            return false;
        }
        data = StorageManager.getStorageManager().getRecords(table);
        System.out.println("Printing data after update...");
        printData(data);

        String alterStmt = "alter table large drop attr3;";
        System.out.println("Stmt: " + alterStmt);
        if (!DDLParser.parseDDLStatement(alterStmt)) {
            System.err.println("Error when altering large table");
            return false;
        }

        table = Catalog.getCatalog().getTable("large");
        System.out.println("Table after alter:");
        System.out.println(table.tableToString());
        System.out.println("Printing data after alter...");
        data = StorageManager.getStorageManager().getRecords(table);
        printData(data);

        alterStmt = "alter table large add attr6 integer default 10;";
        System.out.println("Stmt: " + alterStmt);
        if (!DDLParser.parseDDLStatement(alterStmt)) {
            System.err.println("Error when altering large table");
            return false;
        }

        table = Catalog.getCatalog().getTable("large");
        System.out.println("Table after alter:");
        System.out.println(table.tableToString());
        System.out.println("Printing data after alter...");
        data = StorageManager.getStorageManager().getRecords(table);
        printData(data);

        alterStmt = "alter table large add attr7 char(5) default \"size5\";";
        System.out.println("Stmt: " + alterStmt);
        if (!DDLParser.parseDDLStatement(alterStmt)) {
            System.err.println("Error when altering large table");
            return false;
        }

        table = Catalog.getCatalog().getTable("large");
        System.out.println("Table after alter:");
        System.out.println(table.tableToString());
        System.out.println("Printing data after alter...");
        data = StorageManager.getStorageManager().getRecords(table);
        printData(data);

        String dropTable = "drop table large;";
        System.out.println("Stmt: " + dropTable);
        if (!DDLParser.parseDDLStatement(dropTable)) {
            System.err.println("Error when dropping large table");
            return false;
        }

        table = Catalog.getCatalog().getTable("large");
        if (table != null) {
            System.err.println("Error did not drop the table");
            return false;
        }
        System.out.println("Table dropped successfully.");

        return true;
    }


    ////////////////////////////////////////////

    ////////////////////////////////////////////

    ////////////////////////////////////////////

    ////////////////////////////////////////////
    private static boolean testConstraints() {
        String createTable = "create table table1( attr1 integer, attr2 double notnull, attr3 char(5), primarykey( attr1 ) );";
        System.out.println("Create table stmt:\n" + createTable);

        if (!DDLParser.parseDDLStatement(createTable)) {
            System.err.println("Something went wrong creating the table");
            return false;
        }

        ITable table = Catalog.getCatalog().getTable("table1");
        if (table == null) {
            System.err.println("There was an error getting/creating table");
            return false;
        }

        //should succeed
        String insertStmt = "insert into table1 values " +
                "(1, 3.2, \"foo\"), (7, 5.6, \"bar\"), (3, 11.1, \"baz\");";

        System.out.println("Stmt: " + insertStmt);

        if (!DMLParser.parseDMLStatement(insertStmt)) {
            System.err.println("Something went wrong inserting valid data in the table");
            return false;
        }

        //should fail, inserting null in not null
        insertStmt = "insert int table1 values " +
                "(34, null, null );";

        System.out.println("Stmt: " + insertStmt);

        if (DMLParser.parseDMLStatement(insertStmt)) {
            System.err.println("Something went wrong inserting null in notnull attr");
            return false;
        }

        //should fail, dup primarykey
        insertStmt = "insert int table1 values " +
                "(1, 11.3, \"who\");";

        System.out.println("Stmt: " + insertStmt);

        if (DMLParser.parseDMLStatement(insertStmt)) {
            System.err.println("Something went wrong inserting duplicate pk in the table");
            return false;
        }

        //should fail, wrong data type
        insertStmt = "insert int table1 values " +
                "( 1.5, 11.3, \"who\" );";

        System.out.println("Stmt: " + insertStmt);

        if (DMLParser.parseDMLStatement(insertStmt)) {
            System.err.println("Something went wrong inserting wrong data type in the table");
            return false;
        }

        //should fail, string too long
        insertStmt = "insert int table1 values " +
                "(10, 11.3, \"whoose\" );";

        System.out.println("Stmt: " + insertStmt);

        if (DMLParser.parseDMLStatement(insertStmt)) {
            System.err.println("Something went wrong inserting to large of a string in the table");
            return false;
        }


        //should fail, setting not null to null
        String updateStmt = "update table1 set attr2 = null where attr1 < 10;";

        System.out.println("Stmt: " + updateStmt);
        System.out.println(StorageManager.getStorageManager().getRecords(table));
        if (DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println("Something went wrong updating a notnull to null in the table");
            return false;
        }
        System.out.println(StorageManager.getStorageManager().getRecords(table));

        //should fail, not such table
        updateStmt = "update table5 set attr2 = null where attr1 < 10;";

        System.out.println("Stmt: " + updateStmt);

        if (DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println("Something went wrong updating a non-existing table");
            return false;
        }

        //should fail, no such attr
        updateStmt = "update table1 set attr11 = 10 where attr1 < 10;";

        System.out.println("Stmt: " + updateStmt);

        if (DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println("1 Something went wrong updating a non-existing attr");
            return false;
        }

        //should fail, no such attr
        updateStmt = "update table1 set attr1 = 10 where attr11 < 10;";

        System.out.println("Stmt: " + updateStmt);

        if (DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println(" 2 Something went wrong updating a non-existing attr in where");
            return false;
        }

        //should fail, no such attr
        updateStmt = "update table1 set attr1 = attr11 where attr11 < 10;";

        System.out.println("Stmt: " + updateStmt);

        if (DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println("Something went wrong updating a non-existing attr in where");
            return false;
        }

        //should pass
        updateStmt = "update table1 set attr3 = null where attr1 < 10;";

        System.out.println("Stmt: " + updateStmt);

        if (!DMLParser.parseDMLStatement(updateStmt)) {
            System.err.println("Something went wrong updating a nullable to null in the table");
            return false;
        }

        String fkTable = "create table table2( attr7 integer primarykey, " +
                "foreignkey( attr7 ) references table1( attr1 ) );";

        if (!DDLParser.parseDDLStatement(fkTable)) {
            System.err.println("Something went wrong creating the fk table");
            return false;
        }

        //should pass
        System.out.println();
        insertStmt = "insert into table2 values " +
                "(1);";

        System.out.println("Stmt: " + insertStmt);

        if (!DMLParser.parseDMLStatement(insertStmt)) {
            System.err.println("1 Something went wrong inserting a valid fk value in the table");
            return false;
        }

        //should fail, no such fk value in table1
        insertStmt = "insert int table2 values " +
                "(300);";

        System.out.println("Stmt: " + insertStmt);

        if (DMLParser.parseDMLStatement(insertStmt)) {
            System.err.println("7 Something went wrong inserting a invalid fk value in the table");
            return false;
        }

        return true;
    }

    public static void main(String[] args) {
        Catalog.createCatalog(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        StorageManager.createStorageManager();

        if (!createVeryLargeTable()) {
            System.err.println("Large table test failed.");
        } else {
            System.out.println("Large table test passed.");
        }
        System.out.println("------------------------------------------------");
        System.out.println("------------------------------------------------");
        System.out.println("------------------------------------------------");
        System.out.println("------------------------------------------------");

        if (!testConstraints()) {
            System.err.println("Constraints test failed");
        } else {
            System.out.println("Constraints test passed.");
        }
    }
}