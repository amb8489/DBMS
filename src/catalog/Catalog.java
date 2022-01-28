package catalog;

import common.Attribute;
import common.ITable;

import java.util.ArrayList;

public class Catalog extends ACatalog{

    public Catalog(String location, int pageSize, int pageBufferSize) {

    }

    @Override
    public String getDbLocation() {
        return null;
    }

    @Override
    public int getPageSize() {
        return 0;
    }

    @Override
    public int getPageBufferSize() {
        return 0;
    }

    @Override
    public boolean containsTable(String tableName) {
        return false;
    }

    @Override
    public ITable addTable(String tableName, ArrayList<Attribute> attributes, Attribute primaryKey) {
        return null;
    }

    @Override
    public ITable getTable(String tableName) {
        return null;
    }

    @Override
    public boolean dropTable(String tableName) {
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
