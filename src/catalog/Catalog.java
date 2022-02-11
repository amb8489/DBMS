/*
  Implementation of the catalog interface.
  @author Aaron Berghash (amb8489@rit.edu)

 */


package catalog;

import common.Attribute;
import common.ITable;
import common.Table;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Catalog extends ACatalog {



    // db location
    private String location;
    private int pageSize;
    private int pageBufferSize;
    // string table name    the table
    private HashMap<String, ITable> CurrentTablesInBD;

    //TODO
    private HashMap<Integer, String> PageToTable;



    public Catalog(String location, int pageSize, int pageBufferSize) {

        System.out.println("attempting to find catalog in: "+location);

        // atempt to read catalog file from DB if its there
        try {
            // read in streams
            FileInputStream inputStream;
            inputStream = new FileInputStream(location);
            DataInputStream dataInputStr = new DataInputStream(inputStream);

            System.out.println("found catalog .. restoring");

            // we know this is the location b/c its given
            this.location = location;

            // read in page size
            this.pageSize = dataInputStr.readInt();

            this.pageBufferSize = dataInputStr.readInt();


            int numTables = dataInputStr.readInt();

            // read in tables
            ArrayList<ITable> tabs = Table.ReadAllTablesFromDisk();

            // TODO DO THINGS WITH THE TABLE DATA


        } catch (IOException e) {
            // failure to find page or read fail

            System.out.println("NO catalog in disk.. making new catalog");
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
            // TODO  remove all pages and information stored about the table.

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
    TODO store what pages belong to what tables

    structure is

    [int: pageSize | int: pageBufferSize | int: number of tables]
    saves tables to tables page

     */


    @Override
    public boolean saveToDisk() {


        //TODO
        return false;

    }
}
