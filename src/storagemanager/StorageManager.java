package storagemanager;

import common.ITable;

import java.util.ArrayList;

public class StorageManager extends AStorageManager{

    //TODO
    @Override
    public boolean clearTableData(ITable table) {
        return false;
    }

    //TODO
    @Override
    public ArrayList<Object> getRecord(ITable table, Object pkValue) {
        return null;
    }

    //TODO
    @Override
    public ArrayList<ArrayList<Object>> getRecords(ITable table) {
        return null;
    }

    //TODO
    @Override
    public boolean insertRecord(ITable table, ArrayList<Object> record) {
        return false;
    }

    //TODO
    @Override
    public boolean deleteRecord(ITable table, Object primaryKey) {
        return false;
    }

    //TODO
    @Override
    public boolean updateRecord(ITable table, ArrayList<Object> oldRecord, ArrayList<Object> newRecord) {
        return false;
    }

    //TODO
    @Override
    public void purgePageBuffer() {

    }

    //TODO
    @Override
    public boolean addAttributeValue(ITable table, Object defaultValue) {
        return false;
    }

    //TODO
    @Override
    public boolean dropAttributeValue(ITable table, int attrIndex) {
        return false;
    }
}
