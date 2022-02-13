/*
Kyle Ferguson, Aaron Berghash
 */

package storagemanager;

import catalog.Catalog;
import common.Attribute;
import common.ITable;
import common.Page;
import common.Table;
import pagebuffer.PageBuffer;
import java.io.File;

import java.util.ArrayList;

public class StorageManager extends AStorageManager{


    private static PageBuffer pb;
    public StorageManager(){
        pb = new PageBuffer(Catalog.getCatalog().getPageBufferSize());
    }

    //TODO
    @Override
    public boolean clearTableData(ITable table) {
        if(table instanceof Table workingTable){  //cool piece of code IntelliJ made for me.
            // workingTable is a "patter var" https://openjdk.java.net/jeps/394
            ArrayList<Integer> tablePages = workingTable.getPagesThatBelongToMe();
            for(int page: tablePages){
                String filename = String.format("DB\\pages\\%d", page);
                File tableFile = new File(filename);
                try{
                    if(tableFile.delete()){
                        System.out.printf("Deleted %s\n", filename);
                    }
                    else {
                        System.out.printf("Did not delete %s\n", filename);
                    }
                }
                catch(Exception e){
                    System.err.printf("Failed to delete %s\n", filename);
                    System.err.println(e);
                    return false;
                }
            }

            Catalog.getCatalog().dropTable(table.getTableName());  // drop schema from catalog
            pb.dropFromBuffer(table);
            return true;
        }

        return false;
    }

    //TODO
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

    @Override
    public ArrayList<ArrayList<Object>> getRecords(ITable table) {

        ArrayList<ArrayList<Object>> RECORDS = new ArrayList<>();

        // page name for head is always at idx zero
        int headPtr = ((Table)table).getPagesThatBelongToMe().get(0);

        // loop though all the tables pages in order
        while(headPtr != -1){

            Page headPage = pb.getPageFromBuffer(""+headPtr,table);
            // add all recs
            RECORDS.addAll(headPage.getPageRecords());
            // next page
            headPtr = headPage.getPtrToNextPage();
        }
        return RECORDS;
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


//                headPage.wasChanged = true;

            }

            for(ArrayList<Object> row: headPage.getPageRecords()){


                // TODO compare types

//                if (record.get(pkidx) < row.get(pkidx)){
//                    headPage.getPageRecords().add(idx-1,row);
//                    headPage.wasChanged = true;

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
                    headPage.wasChanged = true;
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

//        headPage.wasChanged = true;

        return false;
    }

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
