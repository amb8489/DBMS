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

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class StorageManager extends AStorageManager {


    private static PageBuffer pb;

    public StorageManager() {
        pb = new PageBuffer(Catalog.getCatalog().getPageBufferSize());
    }


    @Override
    public boolean clearTableData(ITable table) {
        if (table instanceof Table workingTable) {  //cool piece of code IntelliJ made for me.
            // workingTable is a "patter var" https://openjdk.java.net/jeps/394
            ArrayList<Integer> tablePages = workingTable.getPagesThatBelongToMe();
            for (int page : tablePages) {
                String filename = String.format("DB\\pages\\%d", page);
                File tableFile = new File(filename);
                try {
                    if (tableFile.delete()) {
                        System.out.printf("Deleted %s\n", filename);
                    } else {
                        System.out.printf("Did not delete %s\n", filename);
                    }
                } catch (Exception e) {
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

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();

        // loop though all the tables pages in order
        Page headPage = null;
        while (headPtr != -1) {

            headPage = pb.getPageFromBuffer("" + headPtr, table);
            // look though all record for that page

            int idx = 0;

            if (headPage.getPageRecords().size() == 0) {

                headPage.getPageRecords().add(record);
                headPage.wasChanged = true;
                return true;
            }

            for (ArrayList<Object> row : headPage.getPageRecords()) {

                //SUSS find better way in futuer
                int pkid = ((Table) table).pkIdx();
                String pk_type = table.getAttributes().get(pkid).getAttributeType();

                switch (pk_type.charAt(0)) {
                    case 'I':
                        if (((Integer)record.get(pkidx)).compareTo((Integer)row.get(pkidx)) < 0) {
                            headPage.getPageRecords().add(idx, record);
                            headPage.wasChanged = true;
                            return true;
                        }
                        break;
                    case 'D':
                        if (((Double)record.get(pkidx)).compareTo((Double)row.get(pkidx)) < 0) {
                            headPage.getPageRecords().add(idx, record);
                            headPage.wasChanged = true;
                            return true;
                        }
                        break;

                    case 'B':
                        if (((Boolean)record.get(pkidx)).compareTo((Boolean)row.get(pkidx)) < 0) {
                            headPage.getPageRecords().add(idx, record);
                            headPage.wasChanged = true;
                            return true;
                        }
                        break;

                    default:
                        if ((record.get(pkidx).toString()).compareTo(row.get(pkidx).toString()) < 0) {
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
                    System.out.println("REMOVING"+row);

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
                if (oldRecord.get(pkidx) == currRec.get(pkidx)) {
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
