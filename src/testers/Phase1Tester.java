package testers;

/*
  First iteration of the phase 1 tester.

  More will be added to this when actually testing your code.
  More will be released as time permits.

  Please report any issues

  Note: Passing these test does not guarantee a good grade.
  There are edge cases and errors that are not tested here.

  @author: Scott C Johnson (sxjcs@rit.edu)
 */

import catalog.ACatalog;
import common.Attribute;
import common.ITable;
import storagemanager.AStorageManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;

public class Phase1Tester {

    private static ACatalog catalog;
    private static AStorageManager sm;

    /**
     * This function tests to see if the catalog created in the main was created properly and store properly
     * @param location the location of the database
     * @param pageSize the page size
     * @param bufferSize the bufferSize
     */
    private static boolean testCatalogCreation(String location, int pageSize, int bufferSize){
        System.out.println("Testing the catalog...");
        //Create the catalog. This assumes there is no database at the location provided
        catalog = ACatalog.getCatalog();

        if(catalog == null){
            System.err.println("Test Failed: failed to get a catalog");
            System.err.println("Expected catalog, got null");
            return false;
        }

        if(!catalog.getDbLocation().equals(location)){
            System.err.println("Database location does not match in new catalog.");
            System.err.println("Expected: " + location);
            System.err.println("Got: " + catalog.getDbLocation());
            return false;
        }

        if(catalog.getPageSize() != pageSize){
            System.err.println("Database pageSize does not match in new catalog.");
            System.err.println("Expected: " + pageSize);
            System.err.println("Got: " + catalog.getPageSize());
            return false;
        }

        if(catalog.getPageBufferSize() != bufferSize){
            System.err.println("Database bufferSize does not match in new catalog.");
            System.err.println("Expected: " + bufferSize);
            System.err.println("Got: " + catalog.getPageBufferSize());
            return false;
        }

        System.out.println("\tCatalog created successfully");
        return true;
    }

    /**
     * Tests adding a table to the catalog.
     *
     * Assumes the table does not already exist in the catalog
     * @param name the name of the table to add
     * @param attrs the attributes of the table
     * @param pk the primary key of the table
     * @param schema the schema of the table
     */
    private static boolean testCatalogAddTable(String name, ArrayList<Attribute> attrs, Attribute pk, String schema){

        System.out.println("\nTesting adding table to catalog");
        System.out.println("\tSchema: " + schema);
        System.out.println("\tAdding table\n");

        //Look for a table before adding it. It should not be there
        System.out.println("\ttesting containsTable before adding");
        boolean exists = catalog.containsTable(name);

        if(exists){
            System.err.println("Test Failed: table should not exist");
            System.err.println("Expected false, got true");
            return true;
        }

        System.out.println("\ttesting getTable before adding");
        //Get a non-existent table
        //should return null
        //error message should be reported by function
        ITable table1 = catalog.getTable(name);

        if(table1 != null){
            System.err.println("Test Failed: table should not exist");
            System.err.println("Expected null, got table: " + name);
            return true;
        }

        System.out.println("\ttesting dropTable before adding");
        //Drop non-existent table
        //should return false
        //error message should be reported by function
        boolean dropped = catalog.dropTable(name);

        if(dropped){
            System.err.println("Test Failed: table should not exist");
            System.err.println("Expected false, got true");
            return true;
        }

        System.out.println("\ttesting clearTable before adding");
        //clear non-existent table
        //should return false
        //error message should be reported by function
        boolean cleared = catalog.dropTable(name);

        if(cleared){
            System.err.println("Test Failed: table should not exist");
            System.err.println("Expected false, got true");
            return true;
        }

        System.out.println("\t\tEmpty catalog tests passed");

        System.out.println("\tAdding table to catalog...");

        table1 = catalog.addTable(name, attrs, pk);

        if(tableTest(table1, name, attrs, pk)){
            System.err.println("Test failed: failed to add table properly");
            return true;
        }

        System.out.println("\tTesting containsTable");

        if(!catalog.containsTable(name)){
            System.err.println("Test failed: failed to add/contains table properly");
            return true;
        }

        System.out.println("\tTesting getTable");
        table1 = catalog.getTable(name);

        if(tableTest(table1, name, attrs, pk)){
            System.err.println("Test failed: failed to add/get table properly");
            return true;
        }

        System.out.println("\tCatalog Table Schema: ");
        System.out.println(table1.tableToString());

        System.out.println("\tTesting dropTable");

        if(!catalog.dropTable(name)){
            System.err.println("Test failed: failed to drop table properly");
        }

        System.out.println("\tTesting contains and get after drop");
        if(catalog.containsTable(name)){
            System.err.println("Test failed: failed to drop table properly");
        }

        table1 = catalog.getTable(name);

        if(table1 != null){
            System.err.println("Test failed: failed to drop table properly");
        }

        System.out.println("\tTest passed");
        return false;
    }

    /**
     * Tests the storage manager basic functionality
     */
    private static boolean testStorageManager(){
        System.out.println("Testing the storage manager....");
        System.out.println("Creating a table...");

        ArrayList<Attribute> attributes = new ArrayList<>();

        attributes.add(new Attribute("attr1", "Integer"));
        attributes.add(new Attribute("attr2", "Double"));
        attributes.add(new Attribute("attr3", "Boolean"));
        attributes.add(new Attribute("attr4", "Char(5)"));
        attributes.add(new Attribute("attr5", "Varchar(10)"));

        Attribute pk = attributes.get(0);

        System.out.print("tableSM( attr1 Integer pk, attr2 Double, attr3 Boolean, attr4 Char(5), attr5 Varchar(10)");

        ITable table1 = catalog.addTable("table1", attributes, pk);

        System.out.println("Getting table data....");
        System.out.println("\tThere should be none");

        ArrayList<ArrayList<Object>> data = sm.getRecords(table1);

        if(data == null || data.size() != 0){
            System.err.println("Test failed: getRecords result was null or had data in it");
            System.err.println("getting records from an empty table");
            System.err.println("Expected an empty array list since the table was empty");
            return false;
        }

        System.out.println("\tGetting a record from an empty table");
        ArrayList<Object> record = sm.getRecord(table1, 1);

        if(record != null){
            System.err.println("Test failed: getRecord failed");
            System.err.println("getting a non-existent record");
            System.err.println("Expected: null");
            System.err.println("Got: a record");
            return false;
        }

        System.out.println("\tDeleting a record from an empty table");
        boolean deleted = sm.deleteRecord(table1, 1);

        if(deleted) {
            System.err.println("Test failed: deleteRecord failed");
            System.err.println("Deleting a non-existent record");
            System.err.println("Expected: false");
            System.err.println("Got: true");
            return false;
        }

        ArrayList<Object> row1 = new ArrayList<>();
        row1.add(5);
        row1.add(3.2);
        row1.add(true);
        row1.add("hello");
        row1.add("testing");

        ArrayList<Object> row2 = new ArrayList<>();
        row2.add(5);
        row2.add(5.4);
        row2.add(false);
        row2.add("hello");
        row2.add("updated");

        System.out.println("\tUpdating a record from an empty table");

        boolean updated = sm.updateRecord(table1, row1, row2);

        if(updated) {
            System.err.println("Test failed: updateRecord failed");
            System.err.println("Updating a non-existent record");
            System.err.println("Expected: false");
            System.err.println("Got: true");
            return false;
        }

        System.out.println("\tInserting: (5,3.2,true,\"hello\", \"testing\")");
        boolean inserted = sm.insertRecord(table1, row1);

        if(!inserted) {
            System.err.println("Test failed: insertRecord failed");
            System.err.println("inserting a valid record");
            System.err.println("Expected: true");
            System.err.println("Got: false");
            return false;
        }

        System.out.println("\tGetting the record just inserted");
        ArrayList<Object> insertedRow = sm.getRecord(table1, row1.get(0));

        if(!row1.equals(insertedRow)){
            System.err.println("Test failed: inserted record not found");
            System.err.println(insertedRow.toString());
            System.err.println(row1);
            return false;
        }

        System.out.println("\tInserting the same record again... should fail");
        inserted = sm.insertRecord(table1, row1);

        if(inserted) {
            System.err.println("Test failed: insertRecord failed");
            System.err.println("inserting a duplicate record");
            System.err.println("Expected: false");
            System.err.println("Got: true");
            return false;
        }

        System.out.println("\tUpdating the record to: (5,5.4,false,\"hello\", \"updated\")");
        updated = sm.updateRecord(table1, row1, row2);

        if(!updated) {
            System.err.println("Test failed: updateRecord failed");
            System.err.println("Updating a existent record");
            System.err.println("Expected: true");
            System.err.println("Got: false");
            return false;
        }

        System.out.println("\tGetting the updated record");
        ArrayList<Object> updatedRow = sm.getRecord(table1, row1.get(0));

        if(!row2.equals(updatedRow)){
            System.err.println("Test failed: inserted record not found");
        }

        System.out.println("\tDeleting the updated record");
        deleted = sm.deleteRecord(table1, row2.get(0));

        if(!deleted) {
            System.err.println("Test failed: deleteRecord failed");
            System.err.println("Deleting a existent record");
            System.err.println("Expected: true");
            System.err.println("Got: false");
        }
        return true;
    }

    /**
     * Will create a table with the provided information. Will then insert the provided data
     *
     * Assumes no duplicates in the data.
     *
     * Assumes that the table does not already exist
     *
     * @param tableName the name of the table to insert
     * @param attrs the attriobutes of the table
     * @param pk the primary key of the table
     * @param schema the schema of the table
     * @param data the data to insert into the table.
     */
    private static boolean testingInsert(String tableName, ArrayList<Attribute> attrs, Attribute pk,
                                         String schema,
                                         ArrayList<ArrayList<Object>> data){
        System.out.println("Testing the storage manager insert functionality....");
        System.out.println("This is a stress test and to see page splitting");
        System.out.println("This can take some time");
        System.out.println("Creating a table...");
        System.out.println("Table details: \n");
        System.out.println(schema);
        System.out.println();

        ITable table1 = catalog.addTable(tableName, attrs, pk);

        if(table1 == null){
            System.err.println("Test failed trying to create table.");
            return true;
        }

        System.out.println("Table after adding...");
        System.out.println(table1.tableToString());

        if(tableTest(table1, tableName, attrs, pk)){
            System.err.println("Test failed to create table properly");
            return true;
        }

        System.out.println("Getting table data....");
        System.out.println("\tThere should be none");

        ArrayList<ArrayList<Object>> data2 = sm.getRecords(table1);

        if(data2 == null || data2.size() != 0){
            System.err.println("Test failed: getRecords result was null or had data in it");
            System.err.println("getting records from an empty table");
            System.err.println("Expected an empty array list since the table was empty");
            return true;
        }

        System.out.print("\tInserting data..");
        int count = 0;
        for(ArrayList<Object> row: data){
            sm.insertRecord(table1, row);
            if(count++ == 500) {
                System.out.print(".");
                count = 0;
            }
        }

        System.out.println();

        data2 = sm.getRecords(table1);

        if(data2.size() != data.size()){
            System.err.println("Test failed: not enough values inserted");
            System.err.println("Expected: " + data.size());
            System.err.println("Got: " + data2.size());
        }


        return printData(data2);
    }

    /**
     * Function assumes one of the insertion tests passed and there is data in the table
     *
     * Will randomly select rows and delete them.
     * @param tableName the name of the table to delete items from
     */
    private static boolean testingDeletion(String tableName){

        System.out.println("Testing the storage manager delete functionality....");
        System.out.println("This can take some time");
        System.out.println("Getting the table...");

        ITable table = catalog.getTable(tableName);

        System.out.println("Table details: \n");
        System.out.println(table.tableToString());
        System.out.println();

        ArrayList<ArrayList<Object>> data = sm.getRecords(table);

        printData(data);

        System.out.println("\tGetting up to 10 rows to delete...");

        int count = Math.min(data.size(), 10);

        Random rnd = new Random();
        for(int i = 0; i < count; i++){
            ArrayList<Object> row = data.get(Math.abs(rnd.nextInt()% data.size()));
            System.out.println("\t\tDeleting: " + row);
            boolean deleted = sm.deleteRecord(table, row.get(table.getAttributes().indexOf(table.getPrimaryKey())));

            if(!deleted){
                System.err.println("Test failed. Failed to delete valid row. deleteRecord returned false.");
                return true;
            }

            ArrayList<Object> row2 = sm.getRecord(table, row.get(table.getAttributes().indexOf(table.getPrimaryKey())));

            if(row2 != null){
                System.err.println("Test failed. Failed to delete valid row. getRecord returned non-null");
                return true;
            }
        }

        System.out.println("\tRecords deleted...");

        ArrayList<ArrayList<Object>> data2 = sm.getRecords(table);

        if(data2.size() + count != data.size()){
            System.err.println("Test failed. Not enough records were deleted.");
            return true;
        }
        return printData(data2);
    }

    /**
     * Function assumes one of the insertion tests passed and there is data in the table
     *
     * Will randomly select rows and update them.
     * @param tableName the name of the table to delete items from
     */
    private static boolean testingUpdate(String tableName){

        System.out.println("Testing the storage manager update functionality....");
        System.out.println("This can take some time");
        System.out.println("Some updates can fail if the new value for a primary key already exists.");
        System.out.println("Getting the table...");

        ITable table = catalog.getTable(tableName);

        System.out.println("Table details: \n");
        System.out.println(table.tableToString());
        System.out.println();

        ArrayList<ArrayList<Object>> data = sm.getRecords(table);

        printData(data);

        System.out.println("\tGetting up to 10 rows to update...");

        int count = Math.min(data.size(), 10);

        Random rnd = new Random();
        for(int i = 0; i < count; i++){
            ArrayList<Object> row = data.get(Math.abs(rnd.nextInt()% data.size()));
            System.out.println("\t\tUpdating: " + row);

            ArrayList<Object> row3 = randomlyChangeRow(table, row);

            System.out.println("\t\tUpdating to: " + row3);
            sm.updateRecord(table, row, row3);

            ArrayList<Object> row2 = sm.getRecord(table, row3.get(table.getAttributes().indexOf(table.getPrimaryKey())));
            System.out.println("\t\tAfter update: " + row2);
        }

        System.out.println("\tRecords updated...");

        ArrayList<ArrayList<Object>> data2 = sm.getRecords(table);

        return printData(data2);
    }

    private static boolean printData(ArrayList<ArrayList<Object>> data2) {
        System.out.println("Do you want to print the table entries? (y/n)");
        Scanner scanner = new Scanner(System.in);

        if(scanner.nextLine().equals("y")){
            System.out.println("Data should be in order by primary key\n");
            int rowNum = 1;
            for(ArrayList<Object> row: data2) {
                System.out.printf("Row %d: %s%n", rowNum++, row);
            }
        }
        return false;
    }

    private static ArrayList<Object> randomlyChangeRow(ITable table, ArrayList<Object> row){
        ArrayList<Object> changedRow = new ArrayList<>(row);

        Random rnd = new Random();
        int attrIndex = rnd.nextInt(table.getAttributes().size());

        String dataType = table.getAttributes().get(attrIndex).getAttributeType();
        switch (dataType) {
            case "Integer" -> changedRow.set(attrIndex, rnd.nextInt());
            case "Double" -> changedRow.set(attrIndex, rnd.nextDouble());
            case "Boolean" -> changedRow.set(attrIndex, rnd.nextBoolean());
            default -> {
                int count;
                String num;
                if (dataType.startsWith("Char(")) {
                    num = dataType.replace("Char(", "");
                } else {
                    num = dataType.replace("Varchar(", "");
                }
                num = num.replace(")", "");
                num = num.trim();
                count = Integer.parseInt(num);
                changedRow.set(attrIndex, getSaltString(count));
            }
        }
        return changedRow;
    }

    /**
     * This tests to see if the provided table has the provided name, attributes, pk
     * @param table the table to verify
     * @param name the name that the table should have
     * @param attrs the attributes the table should have
     * @param pk the primary key the table should have
     */
    private static boolean tableTest(ITable table, String name, ArrayList<Attribute> attrs, Attribute pk){

        if(table == null){
            System.err.println("Test failed: table is null");
            return true;
        }

        if(!table.getTableName().equals(name)){
            System.err.println("Test failed: table names do not match");
            System.err.println("Expected: " + name);
            System.err.println("Got: " + table.getTableName());
            return true;
        }

        if(attrs.size() != table.getAttributes().size()){
            System.err.println("Test failed: num attrs in table do not match");
            System.err.println("Expected: " + attrs.size());
            System.err.println("Got: " + table.getAttributes().size());
            return true;
        }

        for(int i = 0; i < attrs.size(); i++){
            Attribute attr1 = attrs.get(i);
            Attribute attr2 = table.getAttributes().get(i);

            if(!attr1.attributeName().equals(attr2.attributeName())){
                System.err.println("Test failed: table attr name does not match");
                System.err.println("Expected: " + attr1.attributeName());
                System.err.println("Got: " + attr2.attributeName());
                return true;
            }

            if(!attr1.getAttributeType().equals(attr2.getAttributeType())){
                System.err.println("Test failed: table attr type does not match");
                System.err.println("Attr name: " + attr1.attributeName());
                System.err.println("Expected: " + attr1.attributeType());
                System.err.println("Got: " + attr2.attributeType());
                return true;
            }
        }

        if(!pk.equals(table.getPrimaryKey())){
            System.err.println("Test failed: table pk does not match");
            System.err.println("Expected: " + pk.attributeName());
            System.err.println("Got: " + table.getPrimaryKey().attributeName());
            return true;
        }

        return false;
    }

    /**
     * Random string creation
     * @param length the length of the random string
     */
    private static String getSaltString(int length) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        return salt.toString();

    }

    public static void main(String[] args) {
        if(args.length != 4){
            System.err.println("Usage: java Phase1Tester [location] [pageSize] [bufferSize] [restart]");
            return;
        }

        //Create the storage manager and catalog
        ACatalog.createCatalog(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        sm = AStorageManager.createStorageManager();
        if(!testCatalogCreation(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2])))
            return;

        if(args[3].equals("true")){
            System.out.println("This run of the tester assumes the database already exists and is populated");
            System.out.println("If this is not true, restart the tester with restart as false");
            ITable table = catalog.getTable("oneAttrInsert");
            System.out.println("Table schema: " + table.tableToString());
            ArrayList<ArrayList<Object>> data = sm.getRecords(table);
            printData(data);
            return;
        }

        System.out.println("This run of the tester assumes the provided database location is empty and exists");
        String name = "table1";
        ArrayList<Attribute> attrs = new ArrayList<>();
        String schema = "table1( attr1 Integer primarykey )";
        attrs.add(new Attribute("attr1", "Integer"));
        if(testCatalogAddTable(name, attrs, attrs.get(0), schema))
            return;

        String name2 = "table2";
        ArrayList<Attribute> attrs2 = new ArrayList<>();
        String schema2 = "table2( attr1 Integer primarykey, attr2 Double )";
        attrs2.add(new Attribute("attr1", "Integer"));
        attrs2.add(new Attribute("attr2", "Double"));
        if(testCatalogAddTable(name2, attrs2, attrs2.get(0), schema2))
            return;

        String name3 = "table3";
        ArrayList<Attribute> attrs3 = new ArrayList<>();
        String schema3 = "table3( attr1 Integer, attr2 Double primarykey )";
        attrs3.add(new Attribute("attr1", "Integer"));
        attrs3.add(new Attribute("attr2", "Double"));
        if(testCatalogAddTable(name3, attrs3, attrs3.get(1), schema3))
            return;

        String name4 = "table4";
        ArrayList<Attribute> attrs4 = new ArrayList<>();
        String schema4 = "table4( attr1 Integer, attr2 Double primarykey, attr3 Boolean, attr4 Char(5), attr5 varchar(10) )";
        attrs4.add(new Attribute("attr1", "Integer"));
        attrs4.add(new Attribute("attr2", "Double"));
        attrs4.add(new Attribute("attr3", "Boolean"));
        attrs4.add(new Attribute("attr4", "Char(5)"));
        attrs4.add(new Attribute("attr5", "Varchar(10)"));
        if(testCatalogAddTable(name4, attrs4, attrs4.get(1), schema4))
            return;

        if(!testStorageManager())
            return;

        ArrayList<ArrayList<Object>> data1 = new ArrayList<>();
        for(int i = 0; i < 10000; i++){
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            data1.add(row);
        }
        String schema5 = "oneAttrInsert( attr1 Integer primarykey )";
        Collections.shuffle(data1);
        if(testingInsert("oneAttrInsert", attrs, attrs.get(0), schema5, data1))
            return;

        ArrayList<Double> doubs = new ArrayList<>();
        for(int i = 0; i < 10000; i++){
            doubs.add(i * .62);
        }
        Collections.shuffle(doubs);
        ArrayList<ArrayList<Object>> data2 = new ArrayList<>();
        for(int i = 0; i < 10000; i++){
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            row.add(doubs.get(i));
            data2.add(row);
        }
        String schema6 = "twoAttrInsert( attr1 Integer primarykey, attr2 Double )";
        Collections.shuffle(data2);
        if(testingInsert("twoAttrInsert", attrs2, attrs2.get(0), schema6, data2))
            return;

        String schema7 = "twoAttrInsert2( attr1 Integer, attr2 Double primarykey)";
        Collections.shuffle(data2);
        if(testingInsert("twoAttrInsert2", attrs2, attrs2.get(1), schema7, data2))
            return;

        String schema8 = "fiveAttrInsert( attr1 Integer, attr2 Double primarykey, " +
                "attr3 Boolean, attr4 Char(5), attr5 varchar(10) )";

        ArrayList<ArrayList<Object>> data3 = new ArrayList<>();
        for(int i = 0; i < 10000; i++){
            ArrayList<Object> row = new ArrayList<>();
            row.add(i);
            Random r = new Random();
            row.add(r.nextDouble());
            row.add(r.nextBoolean());
            row.add(getSaltString(5));
            row.add(getSaltString(Math.abs(r.nextInt())%10+1));
            data3.add(row);
        }

        Collections.shuffle(data3);
        if(testingInsert("fiveAttrInsert", attrs4, attrs4.get(0), schema8, data3))
            return;

        if(testingDeletion("oneAttrInsert"))
            return;

        if(testingUpdate("oneAttrInsert"))
            return;

        if(testingDeletion("twoAttrInsert"))
            return;

        if(testingUpdate("twoAttrInsert"))
            return;

        if(testingDeletion("twoAttrInsert2"))
            return;

        if(testingUpdate("twoAttrInsert2"))
            return;

        if(testingDeletion("fiveAttrInsert"))
            return;

        if(testingUpdate("fiveAttrInsert"))
            return;

        System.out.println("Testing complete");
    }
}
