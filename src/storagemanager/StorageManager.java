/*
Kyle Ferguson, Aaron Berghash
 */

package storagemanager;

import catalog.ACatalog;
import catalog.Catalog;
import common.*;
import pagebuffer.PageBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import filesystem.FileSystem;
import parsers.WhereP3;
import parsers.WhereParser;

public class StorageManager extends AStorageManager {


    public static PageBuffer pb;
    private static WhereP3 wp;

    public StorageManager() {
        pb = new PageBuffer(Catalog.getCatalog().getPageBufferSize());
        wp = new WhereP3();
    }


    public PageBuffer getPagebuffer() {
        return pb;
    }

    @Override
    public boolean clearTableData(ITable table) {
        if (table instanceof Table workingTable) {  //cool piece of code IntelliJ made for me.
            // workingTable is a "patter var" https://openjdk.java.net/jeps/394
            ArrayList<Integer> tablePages = workingTable.getPagesThatBelongToMe();
            for (int page : tablePages) {
                FileSystem.deletePageFile(page);
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


    public boolean TableContainsFkVal(ForeignKey fk, Object wanted) {
        try {
            Table FkTable = (Table) Catalog.getCatalog().getTable(fk.getRefTableName());

            // page name for head is always at idx zero
            int headPtr = ((Table) FkTable).getPagesThatBelongToMe().get(0);

            // where in a row the fk val is


            int fkidx = 0;
            for (Attribute a : FkTable.getAttributes()) {
                if (a.getAttributeName().equals(fk.refAttribute())) {
                    break;
                } else {
                    fkidx++;
                }
            }

            // loop though all the tables pages in order to find
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, FkTable);
                // look though all record for that page
                for (ArrayList<Object> row : headPage.getPageRecords()) {
//                    System.err.println(row.get(fkidx)+"--------------"+fk);

                    if (row.get(fkidx).equals(wanted)) {
                        return true;
                    }
                }
                // next page
                headPtr = headPage.getPtrToNextPage();
            }
            return false;


        } catch (Exception e) {
            System.err.println("could not find val in fk table");
            return false;
        }
    }


    @Override
    public ArrayList<ArrayList<Object>> getRecords(ITable table) {

        ArrayList<ArrayList<Object>> RECORDS = new ArrayList<>();

        // page name for head is always at idx zero
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

        // loop though all the tables pages in order
        while (headPtr != -1) {

            Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);
            // add all recs

            RECORDS.addAll(headPage.getPageRecords());
            // next page
            headPtr = headPage.getPtrToNextPage();
        }

        return RECORDS;
    }


    @Override
    public boolean insertRecord(ITable table, ArrayList<Object> record) {
        try {

            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);


            // where in a row the pk is
            int pkidx = ((Table) table).pkIdx();

            // all string will not have " " at front and end
            int idxx = 0;
            for (Object val : record) {
                String attribType = table.getAttributes().get(idxx).getAttributeType();
                if (attribType.endsWith(")")) {
                    String str = record.get(idxx).toString();
                    if (str != null && Utilities.isStringTooLong(attribType, str)) {
                        System.err.println("string: " + record.get(idxx) + " too long fr type: " + attribType);
                        return false;
                    }
                    if (str != null) {
                        // removing " before plaving in db
                        record.set(idxx, str.replace("\"", ""));
                    }
                }
                idxx++;
            }


            for (Integer i : ((Table) table).indicesOfNotNullAttributes) {
                if (record.get(i) == null) {
                    System.err.println("attribute: " + table.getAttributes().get(i).getAttributeName() + " cant be null");
                    return false;
                }
            }


            HashMap<String, Integer> AttribNamesIdx = new HashMap<>();
            ArrayList<Attribute> attrs = table.getAttributes();
            for (int i = 0; i < table.getAttributes().size(); i++) {
                AttribNamesIdx.put(attrs.get(i).getAttributeName(), i);
            }


            for (ForeignKey fk : (table).getForeignKeys()) {
                String fkAttribute = fk.getAttrName();
                Object valueToFindInFKtab = record.get(AttribNamesIdx.get(fkAttribute));

                if (!TableContainsFkVal(fk, valueToFindInFKtab)) {
                    System.err.println(valueToFindInFKtab + " not in fk " + fk);

                    return false;
                }

            }


            // loop though all the tables pages in order
            Page headPage = null;

            while (headPtr != -1) {


//            VerbosePrint.print("inside: "+headPtr);
//            VerbosePrint.print(((Table) table).getPagesThatBelongToMe());


                headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);


                int numRecords = headPage.getPageRecords().size();
                if (numRecords == 0) {
//                VerbosePrint.print("head page size 0: "+headPtr);

                    headPage.insert(0, record);

                    return true;
                }
//            VerbosePrint.print("here 1: "+headPtr);

                boolean doInsert = false;

                for (int i = 0; i < numRecords; i++) {

                    ArrayList<Object> row = headPage.getPageRecords().get(i);
                    int pkid = ((Table) table).pkIdx();
                    String pk_type = table.getAttributes().get(pkid).getAttributeType().toUpperCase();

                    switch (pk_type.charAt(0)) {
                        case 'I' -> {
                            int resI = ((Integer) record.get(pkidx)).compareTo((Integer) row.get(pkidx));
                            if (resI == 0) {
                                return false;
                            }

                            if (resI < 0) {
                                doInsert = true;
                            }
                        }
                        case 'D' -> {
                            int resD = ((Double) record.get(pkidx)).compareTo((Double) row.get(pkidx));

                            if (resD == 0) {
                                return false;
                            }
                            if (resD < 0) {
                                doInsert = true;
                            }
                        }
                        case 'B' -> {
                            int resB = ((Boolean) record.get(pkidx)).compareTo((Boolean) row.get(pkidx));
                            if (resB == 0) {
                                return false;
                            }
                            if (resB < 0) {
                                doInsert = true;
                            }
                        }
                        default -> {
                            int resS = ((record.get(pkidx).toString()).compareTo(row.get(pkidx).toString()));

                            if (resS == 0) {
                                return false;
                            }
                            if (resS < 0) {
                                doInsert = true;
                            }
                        }
                    }

                    if (doInsert) {
                        return headPage.insert(i, record);
                    }
                }
                // get next page

                headPtr = headPage.getPtrToNextPage();

            }
            // add to last spot in page in table

            return headPage.insert(headPage.getPageRecords().size(), record);


        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
            System.err.println("Storage manager(insertRecord): error in inserting");
            return false;
        }
    }

    public boolean deleteRecordWhere(ITable table, String where, Boolean removeAllRecords) {
        try {


            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

            ArrayList<Attribute> attributes = table.getAttributes();
            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);
                // look though all record for that page

                int recSize = headPage.getPageRecords().size();

                for (int i = recSize - 1; i > -1; i--) {
                    ArrayList<Object> row = headPage.getPageRecords().get(i);
                    if (removeAllRecords || wp.whereIsTrue(where, (Table) table, row)) {
                        headPage.delete(i);
                    }
                }

                // next page
                headPtr = headPage.getPtrToNextPage();
            }
            return true;
        } catch (Exception e) {
            System.err.println("error removing in remove where in sm");
            return false;
        }
    }

    public boolean keepWhere(ITable table, String where, Boolean removeAllRecords) {
        try {
            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

            ArrayList<Attribute> attributes = table.getAttributes();
            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);
                // look though all record for that page

                int recSize = headPage.getPageRecords().size();

                for (int i = recSize - 1; i > -1; i--) {
                    ArrayList<Object> row = headPage.getPageRecords().get(i);
                    if (removeAllRecords || (!wp.whereIsTrue(where, (Table) table, row))) {
                        headPage.delete(i);
                    }
                }

                // next page
                headPtr = headPage.getPtrToNextPage();
            }
            return true;
        } catch (Exception e) {
            System.err.println("error removing in remove where in sm");
            return false;
        }
    }


    @Override
    public boolean deleteRecord(ITable table, Object primaryKey) {

        // page name for head is always at idx zero
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();

        // loop though all the tables pages in order
        while (headPtr != -1) {
            Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);
            // look though all record for that page
            int idx = 0;
            for (ArrayList<Object> row : headPage.getPageRecords()) {
                if (row.get(pkidx).equals(primaryKey)) {
                    headPage.delete(idx);
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

            Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);
            // look through all record for that page
            int IDX = 0;
            for (ArrayList<Object> currRec : headPage.getPageRecords()) {
                if (oldRecord.get(pkidx).toString().equals(currRec.get(pkidx).toString())) {

                    ArrayList<Object> deletedRec = headPage.getPageRecords().get(IDX);
                    headPage.delete(IDX);

                    boolean successfulInsert = insertRecord(table, newRecord);

                    // restore
                    if (!successfulInsert) {
                        insertRecord(table, deletedRec);
                    }

                    return successfulInsert;
                }
                IDX++;
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

//    @Override
//    public boolean addAttributeValue(ITable table, Object defaultValue) {
//
//
//
//
//        try {
//
//
//            // page name for head is always at idx zero
//            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);
//
//
////            System.exit(2);
//
//            // loop though all the tables pages in order
//
//            int next = 1;
//            while (headPtr != -1) {
//
//                Page headPage = null;
//
//                System.out.println(headPtr);
//
//                // if page is not loaded in yet it needs to be before we add the new attrb
//                // asssumes its already been added by the time this function funtion runs
//                if (!pb.isPageInBuffer(String.valueOf(headPtr))){
//
//                    //get last
//                    Attribute newAtter = table.getAttributes().remove(table.getAttributes().size()-1);
//
//
//                    //load page into buffer
//                    pb.getPageFromBuffer(String.valueOf(headPtr), table);
//
//
//                    // readd attrib
//                    table.getAttributes().add(newAtter);
//
//
//                }
//
//
//                headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);
//
//
//                // next page before we split
//
//                headPtr =headPage.getPtrToNextPage();
//
//                //                                                      hmmmmmmm v
//                ArrayList<ArrayList<Object>> tempRecs = new ArrayList<>(headPage.getPageRecords());
//                headPage.ClearRecords();
//
//                //add the new atttribute to all the recs
//
//                // recs being double added
//
//                tempRecs.forEach(row -> row.add(defaultValue));
//
//
//
//                // reinsert into page
//
//                for (ArrayList<Object> tempRec : tempRecs) {
//                    headPage.insert(tempRec);
//                }
//
//            }
//
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.err.println("failure adding attribute from table " + table.getTableName());
//            return false;
//        }
//    }
    @Override
    public boolean addAttributeValue(ITable table, Object defaultValue) {




        try {

            System.out.println(((Table) table).getPagesThatBelongToMe());

            // page name for head is always at idx zero
            ArrayList<Integer> pages = ((Table) table).getPagesThatBelongToMe();
            int headPtr = pages.get(0);
            Table Clone = new Table(table);

            Clone.getPagesThatBelongToMe().clear();

            Page firstPageForTable = new Page(Clone);
            Clone.getPagesThatBelongToMe().add(Integer.valueOf(firstPageForTable.getPageName()));
            firstPageForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), Clone);


            //TODO OPTIMISE BY SETTING ALL PAGES THAT BELONG TO OLD TABLE TO WASNOT CHANGED in PageBuffer

            // or just clone the table and clear the table data and page by page from old table
            // re insert into new empty clone

            table.getAttributes().remove(table.getAttributes().size()-1);

            while (headPtr != -1) {


                // if page is not loaded in yet it needs to be before we add the new attrb
                // asssumes its already been added by the time this function funtion runs

                Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);

                // next page before we split

//                List<ArrayList<Object>> newRecs = new ArrayList<>(headPage.getPageRecords());

//                headPage.getPageRecords().clear();

                // whast happening is that when we insert into new tabele clone
                // old pages from old table are being pushed out of page buffer  and trying to be written with nnew rows


                // cloning old table page records
                ArrayList<ArrayList<Object>> newRecs = new ArrayList<>();

                for(ArrayList<Object> row : headPage.getPageRecords()){
                    newRecs.add(new ArrayList<>(row));
                }
                // adding new value

                newRecs.forEach(row -> row.add(defaultValue));



                // reinsert into new table

                for (ArrayList<Object> tempRec : newRecs) {
                    StorageManager.getStorageManager().insertRecord(Clone,tempRec);
                }


                // getting next page from old table
                headPtr = headPage.getPtrToNextPage();

            }

            String ClonesNewName = table.getTableName();
            // DROP old TABLE MAKE SURE ALL OLD PAGES IN HARDWEAR ARE REMOVED
            StorageManager.getStorageManager().clearTableData(table);
            // set new table name to old table
            Clone.setTableName(ClonesNewName);

            // ADD CLONE TABLE TO CATALOG
            ((Catalog) Catalog.getCatalog()).addExistingTable(Clone);


            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("failure adding attribute from table " + table.getTableName());
            return false;
        }
    }


    //TODO UPDATE PAGE SIZE based on record
    @Override
    public boolean dropAttributeValue(ITable table, int attrIndex) {
        try {

            if (attrIndex >= table.getAttributes().size()) {
                System.err.println("Table cant remove attribute at index " + attrIndex + " because" +
                        " table doesn't have that many attributes");
                return false;
            }
            // update table first
            table.dropAttribute(table.getAttributes().get(attrIndex).getAttributeName());
            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);
                // delete that col from all recs

                // for each row in rows remove row[i] from the row
                headPage.getPageRecords().forEach(row -> row.remove(attrIndex));
                headPage.wasChanged = true;
                // next page
                headPtr = headPage.getPtrToNextPage();
            }


            return true;
        } catch (Exception e) {
            System.err.println("failure removing attribute at index " + attrIndex + " from table " + table.getTableName());
            return false;
        }
    }
}
