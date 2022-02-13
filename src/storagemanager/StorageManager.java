package storagemanager;

import catalog.Catalog;
import common.Attribute;
import common.ITable;
import common.Page;
import common.Table;
import pagebuffer.PageBuffer;

import java.util.ArrayList;

public class StorageManager extends AStorageManager{


    PageBuffer pb;
    public StorageManager(){
        this.pb = new PageBuffer(Catalog.getCatalog().getPageBufferSize());
    }

    //TODO
    @Override
    public boolean clearTableData(ITable table) {
        return false;
    }

    @Override
    public ArrayList<Object> getRecord(ITable table, Object pkValue) {

        // page name for head is always at idx zero
        int headPtr = ((Table)table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();



        // loop though all the tables pages in order
        while(headPtr != -1){

            Page headPage = pb.getPageFromBuffer(""+headPtr,table);
            // look though all record for that page
            for(ArrayList<Object> row: headPage.getPageRecords()){
                if (row.get(pkidx).equals(pkValue)){
                    return row;
                }
            }
            // next page
            headPtr = headPage.getPtrToNextPage();
        }
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

        // page name for head is always at idx zero
        int headPtr = ((Table)table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();



        // loop though all the tables pages in order
        while(headPtr != -1){

            Page headPage = pb.getPageFromBuffer(""+headPtr,table);
            // look though all record for that page

            int idx = 0;

            //TODO if page is empty
            if (headPage.getPageRecords().size()==0){



            }

            for(ArrayList<Object> row: headPage.getPageRecords()){


                // TODO compare types

//                if (record.get(pkidx) < row.get(pkidx)){
//                    headPage.getPageRecords().add(idx-1,row);
//                    return true;
//                }


                idx++;
            }
            // next page
            headPtr = headPage.getPtrToNextPage();
        }
        return false;
    }

    @Override
    public boolean deleteRecord(ITable table, Object primaryKey) {

        // page name for head is always at idx zero
        int headPtr = ((Table)table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();

        // loop though all the tables pages in order
        while(headPtr != -1){

            Page headPage = pb.getPageFromBuffer(""+headPtr,table);
            // look though all record for that page
            int idx = 0;
            for(ArrayList<Object> row: headPage.getPageRecords()){
                if (row.get(pkidx).equals(primaryKey)){
                    headPage.getPageRecords().remove(idx);
                    return true;
                }
                idx++;
            }
            // next page
            headPtr = headPage.getPtrToNextPage();
        }
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
        pb.PurgeBuffer();

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
