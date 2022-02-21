/*
Kyle Ferguson, Aaron Berghash
 */

package storagemanager;

import catalog.Catalog;
import common.*;
import pagebuffer.PageBuffer;

import java.io.File;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import filesystem.FileSystem;
import java.util.List;

public class StorageManager extends AStorageManager {


    private static PageBuffer pb;

    public StorageManager() {
        pb = new PageBuffer(Catalog.getCatalog().getPageBufferSize());
    }


    //TODO
    @Override
    public boolean clearTableData(ITable table) {
        if (table instanceof Table workingTable) {  //cool piece of code IntelliJ made for me.
            // workingTable is a "patter var" https://openjdk.java.net/jeps/394
            ArrayList<Integer> tablePages = workingTable.getPagesThatBelongToMe();
            for(int page: tablePages){
                FileSystem.deletePageFile(page);
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
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();


        // loop though all the tables pages in order
        while (headPtr != -1) {

            Page headPage = pb.getPageFromBuffer("" + headPtr, table);
            // look though all record for that page
            for (ArrayList<Object> row : headPage.getPageRecords()) {
                if (row.get(pkidx).equals(pkValue)) {
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
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

        // loop though all the tables pages in order
        while (headPtr != -1) {

            Page headPage = pb.getPageFromBuffer("" + headPtr, table);
            // add all recs

            RECORDS.addAll(headPage.getPageRecords());
            // next page
            headPtr = headPage.getPtrToNextPage();
        }
        return RECORDS;
    }

    @Override
    public boolean insertRecord(ITable table, ArrayList<Object> record) {

        // page name for head is always at idx zero
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);
//        VerbosePrint.print("head page: "+headPtr);
        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();

        // loop though all the tables pages in order
        Page headPage = null;

        while (headPtr != -1) {


//            VerbosePrint.print("inside: "+headPtr);
//            VerbosePrint.print(((Table) table).getPagesThatBelongToMe());

            headPage = pb.getPageFromBuffer("" + headPtr, table);
            // look though all record for that page
//            VerbosePrint.print("got: "+headPtr);
//            VerbosePrint.print(headPage.getPageRecords());


            int idx = 0;

            if (headPage.getPageRecords().size() == 0) {
//                VerbosePrint.print("head page size 0: "+headPtr);

                headPage.getPageRecords().add(record);
                headPage.wasChanged = true;
                return true;
            }
//            VerbosePrint.print("here 1: "+headPtr);

            for (ArrayList<Object> row : headPage.getPageRecords()) {
//                VerbosePrint.print("here 2: "+headPtr+ " with "+record);


                //SUSS find better way in future
                int pkid = ((Table) table).pkIdx();
                String pk_type = table.getAttributes().get(pkid).getAttributeType();

                switch (pk_type.charAt(0)) {
                    case 'I':
                        int resI= ((Integer)record.get(pkidx)).compareTo((Integer)row.get(pkidx));
                        if( resI == 0){
                            return false;
                        }

                        if ( resI< 0) {

                            headPage.getPageRecords().add(idx, record);
                            headPage.wasChanged = true;
                            return true;
                        }
                        break;
                    case 'D':

                        int resD= ((Double)record.get(pkidx)).compareTo((Double)row.get(pkidx));
                        if( resD == 0){
                            return false;
                        }

                        if ( resD< 0) {
                            headPage.getPageRecords().add(idx, record);
                            headPage.wasChanged = true;
                            return true;
                        }
                        break;

                    case 'B':

                        int resB= ((Boolean)record.get(pkidx)).compareTo((Boolean)row.get(pkidx));
                        if( resB == 0){
                            return false;
                        }

                        if ( resB< 0) {
                            headPage.getPageRecords().add(idx, record);
                            headPage.wasChanged = true;
                            return true;
                        }
                        break;

                    default:
                        int resS= ((record.get(pkidx).toString()).compareTo(row.get(pkidx).toString()));
                        if( resS == 0){
                            return false;
                        }
                        if (resS < 0) {
                            headPage.getPageRecords().add(idx, record);
                            headPage.wasChanged = true;
                            return true;
                        }
                        break;
                }

                idx++;
            }
            // next page

            headPtr = headPage.getPtrToNextPage();

        }
        // add to last spot in page in table
        headPage.getPageRecords().add(record);
        headPage.wasChanged = true;


        return true;
    }


    @Override
    public boolean deleteRecord(ITable table, Object primaryKey) {

        // page name for head is always at idx zero
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();

        // loop though all the tables pages in order
        while (headPtr != -1) {

            Page headPage = pb.getPageFromBuffer("" + headPtr, table);
            // look though all record for that page
            int idx = 0;
            for (ArrayList<Object> row : headPage.getPageRecords()) {

                if (row.get(pkidx).equals(primaryKey)) {
                    VerbosePrint.print("REMOVING"+row);

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

    /**
     * This  function  will  update  the  provided  record  with  the  data  of  the  new  record.  This  can
     * cause  the  record  to  move  to  a  new  page  if:
     * • the  primary  key  changes
     * • the  size  increases  causing  a  page  split.  This  can  also  cause  other  records  to  move  as
     * well.
     *
     * @param table     the table to update the record in
     * @param oldRecord the old record data
     * @param newRecord the new record data
     * @return
     */
    @Override
    public boolean updateRecord(ITable table, ArrayList<Object> oldRecord, ArrayList<Object> newRecord) {

        // page name for head is always at idx zero
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();

        // loop though all the tables pages in order

        while (headPtr != -1) {

            Page headPage = pb.getPageFromBuffer("" + headPtr, table);
            // look through all record for that page

            for (ArrayList<Object> currRec : headPage.getPageRecords()) {
                if (oldRecord.get(pkidx).toString().equals(currRec.get(pkidx).toString())) {
                    headPage.getPageRecords().remove(oldRecord);
                    headPage.wasChanged = true;
                    return insertRecord(table, newRecord);
                }
            }
            // next page
            headPtr = headPage.getPtrToNextPage();
        }

        return false;
    }

    @Override
    public void purgePageBuffer() {
        pb.PurgeBuffer();
    }

    //TODO TEST how do we know what the defaultValue type is????
    // how do we update the attribute array in table????
    @Override
    public boolean addAttributeValue(ITable table, Object defaultValue) {

        try {


            // TODO need to do this first or at some point before
            //  Attribute newAttrib = new Attribute(...);
            //  Catalog.getCatalog().getTable(table.getTableName()).getAttributes().add(newAttrib);

            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);
                // delete that col from all recs

                headPage.getPageRecords().forEach(r -> r.add(defaultValue));

                // next page
                headPtr = headPage.getPtrToNextPage();
            }


            return true;
        }catch (Exception e){
            System.err.println("failure adding attribute from table "+table.getTableName());
            return false;
        }
    }

    //TODO TEST reading and writing from updated page
    //TODO test removing out of bounds of attrib array
    @Override
    public boolean dropAttributeValue(ITable table, int attrIndex) {

        try {

            if (attrIndex>=table.getAttributes().size()){
                System.err.println("Table cant remove attribute at index "+attrIndex+" because" +
                        " table doesn't have that many attributes");
                return false;
            }
            // update table first
            Catalog.getCatalog().getTable(table.getTableName()).getAttributes().remove(attrIndex);

            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);
                // delete that col from all recs

                headPage.getPageRecords().forEach(r -> r.remove(attrIndex));
                // next page
                headPtr = headPage.getPtrToNextPage();
            }


            return true;
        }catch (Exception e){
            System.err.println("failure removing attribute at index "+attrIndex+" from table "+table.getTableName());
            return false;
        }
    }
}
