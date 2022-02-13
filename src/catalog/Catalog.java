/*
  Implementation of the catalog interface.
  @author Aaron Berghash (amb8489@rit.edu)

 */


package catalog;

import common.Attribute;
import common.ITable;
import common.Table;
import common.VerbosePrint;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class Catalog extends ACatalog {



    // db location
    private String location;
    private int pageSize;
    private int pageBufferSize;
    // string table name    the table
    public HashMap<String, ITable> CurrentTablesInBD = new HashMap<>();



    public Catalog(String location, int pageSize, int pageBufferSize) {

        VerbosePrint.print("attempting to find catalog in: "+location+"/catalog/catalog.txt");

        // atempt to read catalog file from DB if its there


        try {
            // read in streams
            FileInputStream inputStream;
            inputStream = new FileInputStream(location+"/catalog/catalog.txt");
            DataInputStream dataInputStr = new DataInputStream(inputStream);

            VerbosePrint.print("found catalog .. restoring");

            // we know this is the location b/c its given
            this.location = location;
            // read in page size
            this.pageSize = dataInputStr.readInt();

            // read in pageBufferSize
            this.pageBufferSize = dataInputStr.readInt();

            // read in tables.txt
            //TODO ADD LOCATION TO THIS
            ArrayList<ITable> tabs = Table.ReadAllTablesFromDisk(location);

            if( tabs!=null && tabs.size() >0) {
                // restoring tale data
                for (ITable table : tabs) {
                    CurrentTablesInBD.put(table.getTableName(), table);
                }
            }
            VerbosePrint.print("restore successful! ");

        } catch (IOException e) {
            // failure to find page or read fail

            VerbosePrint.print("NO catalog in disk.. making new catalog");
            this.location = location;
            this.pageSize = pageSize;
            this.pageBufferSize = pageBufferSize;
        }
    }


    @Override
    public String getDbLocation() {
        return this.location;
    }

    @Override
    public int getPageSize() {
        return this.pageSize;
    }

    @Override
    public int getPageBufferSize() {
        return this.pageBufferSize;
    }

    @Override
    public boolean containsTable(String tableName) {
        return this.CurrentTablesInBD.containsKey(tableName);
    }


    // add table to CurrentTablesInBD
    @Override
    public ITable addTable(String tableName, ArrayList<Attribute> attributes, Attribute primaryKey) {
        // table already exist or not
        if (containsTable(tableName)) {
            System.err.println(String.format("table with name %s is already taken", tableName));
            return null;
        }

        // mk table
        ITable newTable = new Table(tableName, attributes, primaryKey);
        // add to CurrentTablesInBD  name --> table
        this.CurrentTablesInBD.put(tableName, newTable);
        return newTable;
    }

    @Override
    public ITable getTable(String tableName) {
        if (containsTable(tableName)) {
            return CurrentTablesInBD.get(tableName);
        }
        System.err.println(String.format("table with name %s is does not exist", tableName));

        return null;
    }

    @Override
    public boolean dropTable(String tableName) {

        // table already exist or not
        if (containsTable(tableName)) {
            this.CurrentTablesInBD.remove(tableName);

            return true;
        }
        System.err.println(String.format("table with name %s is does not exist", tableName));

        return false;
    }

    @Override
    public boolean alterTable(String tableName, Attribute attr, boolean drop, Object defaultValue) {
        return false;
    }

    @Override
    public boolean clearTable(String tableName) {
        return false;
    }

    @Override
    public boolean addIndex(String tableName, String indexName, String attrName) {
        return false;
    }

    @Override
    public boolean dropIndex(String tableName, String indexName) {
        return false;
    }



    /*

    structure is

    1)int: pageSize | int: pageBufferSize | int: number of tables.txt]

    2)saves tables.txt to tables.txt page in DB

     */


    @Override
    public boolean saveToDisk() {

        try {

            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("src/DB/catalog/catalog.txt")));
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();


            // write in page size
            outputStream.write(ByteBuffer.allocate(4).putInt(this.pageSize).array());

            // write in page buffer size
            outputStream.write(ByteBuffer.allocate(4).putInt(this.pageBufferSize).array());

            out.write(outputStream.toByteArray());
            out.close();


            /////// write all tables out to "src/DB/catalog/tables.txt"
            if(CurrentTablesInBD!= null && CurrentTablesInBD.size() >0 ) {

                out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Catalog.getCatalog().getDbLocation()+"/tabs/tables.txt")));
            outputStream = new ByteArrayOutputStream();

                for (String tableName : CurrentTablesInBD.keySet()) {
                    Table t = (Table) CurrentTablesInBD.get(tableName);
                    outputStream.write(t.toBytes());
                }

            out.write(outputStream.toByteArray());
            out.close();
            }


            return true;
        }catch (IOException i){
            System.err.println("ERROR Saving catalog to disk");
            return false;
        }
    }
}
