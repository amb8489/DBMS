/*
  Implementation of the catalog interface.
  @author Aaron Berghash (amb8489@rit.edu)

 */


package catalog;

import common.Attribute;
import common.ITable;
import common.Table;

import java.util.ArrayList;
import java.util.HashMap;

public class Catalog extends ACatalog{

    // db location
    private String location;

    private int pageSize;
    private int pageBufferSize;

    // string table name    the table
    private HashMap<String,ITable> CurrentTablesInBD;

    public Catalog(String location, int pageSize, int pageBufferSize) {
        this.location = location;
        this.pageSize = pageSize;
        this.pageBufferSize = pageBufferSize;
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
        if (containsTable(tableName)){
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
        if (containsTable(tableName)){
            return CurrentTablesInBD.get(tableName);
        }
        return null;
    }

    @Override
    public boolean dropTable(String tableName) {

        // table already exist or not
        if (containsTable(tableName)){
            this.CurrentTablesInBD.remove(tableName);
            return true;
        }
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

    @Override
    public boolean saveToDisk() {
        return false;
    }
}
