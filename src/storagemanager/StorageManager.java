/*
Kyle Ferguson, Aaron Berghash
 */

package storagemanager;

import catalog.ACatalog;
import catalog.Catalog;
import common.*;
import indexing.BPlusTree;
import pagebuffer.PageBuffer;

import java.util.*;

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
                    String str = (String) record.get(idxx);
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

            // looking for inserting null when it shouldnt be
            for (Integer i : ((Table) table).indicesOfNotNullAttributes) {
                if (record.get(i) == null) {
                    System.err.println("attribute: " + table.getAttributes().get(i).getAttributeName() + " cant be null");
                    return false;
                }
            }


            HashMap<String, Integer> AttribNamesIdx = ((Table) table).AttribIdxs;


            // checking fk in other table will need to optimize search
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


                headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);


                int numRecords = headPage.getPageRecords().size();
                if (numRecords == 0) {
                    headPage.insert(0, record);
                    return true;
                }

                // binary search for pk on page
                var pkIdx = ((Table) table).pkIdx();
                String pk_type = table.getAttributes().get(pkIdx).getAttributeType().toUpperCase();
                int index;


                //TODO if pk is > the the last row for this page then go to next page
                // be careful that next page is -1
//                if(headPage.getPageRecords().get(index)){
//
//
//
//                }


                switch (pk_type.charAt(0)) {
                    case 'I' -> index = Collections.binarySearch(headPage.getPageRecords(), record, Comparator.comparing(row -> (Integer) row.get(pkIdx)));
                    case 'D' -> index = Collections.binarySearch(headPage.getPageRecords(), record, Comparator.comparing(row -> (Double) row.get(pkIdx)));
                    case 'B' -> index = Collections.binarySearch(headPage.getPageRecords(), record, Comparator.comparing(row -> (Boolean) row.get(pkIdx)));
                    default -> index = Collections.binarySearch(headPage.getPageRecords(), record, Comparator.comparing(row -> (String) row.get(pkIdx)));
                }


                // if we should insert last i.e records.size() then go to next page
                // else if it says to insert then check at that location no dubs exist (error)
                // if on last page and for last element then add to end of page

                if (index >= 0) {
                    // check for dup
                        System.err.println("duplicate primary key value trying to be insert");
                        return false;

                } else {
                    index = -index - 1;

                    // wanting to inset into the last element of the page
                    // go to next page to insert
                    if (index != headPage.getPageRecords().size()) {
                        return headPage.insert(index, record);
                    }else if(headPage.getPtrToNextPage() == -1){
                        return headPage.insert(index, record);
                    }
                }

                headPtr = headPage.getPtrToNextPage();

            }
            // add to last spot in last page in table

            // not sure if we need this anymore
//            System.out.println("here2 end " + headPage.getPageRecords().size());
//
//            return headPage.insert(headPage.getPageRecords().size(), record);


        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
            System.err.println("Storage manager(insertRecord): error in inserting");
            return false;
        }
        return false;
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

    @Override
    public boolean addAttributeValue(ITable table, Object defaultValue) {


        try {


            // page name for head is always at idx zero
            ArrayList<Integer> pages = ((Table) table).getPagesThatBelongToMe();
            int headPtr = pages.get(0);

            Table Clone = new Table(table);
            Clone.getPagesThatBelongToMe().clear();

            Page firstPageForTable = new Page(Clone);
            Clone.getPagesThatBelongToMe().add(Integer.valueOf(firstPageForTable.getPageName()));
            firstPageForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), Clone);


            table.getAttributes().remove(table.getAttributes().size() - 1);


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

                for (ArrayList<Object> row : headPage.getPageRecords()) {
                    newRecs.add(new ArrayList<>(row));
                }

                // adding new value


                newRecs.forEach(row -> row.add(defaultValue));


                // reinsert into new table

                for (ArrayList<Object> tempRec : newRecs) {
                    StorageManager.getStorageManager().insertRecord(Clone, tempRec);
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
            if (attrIndex == ((Table) table).pkIdx()) {
                System.err.println("ERROR: tryig to drop the primary key");
                return false;
            }

            // page name for head is always at idx zero
            ArrayList<Integer> pages = ((Table) table).getPagesThatBelongToMe();
            int headPtr = pages.get(0);

            Table Clone = new Table(table);
            Clone.dropAttribute(Clone.getAttributes().get(attrIndex).getAttributeName());
            Clone.getPagesThatBelongToMe().clear();

            Page firstPageForTable = new Page(Clone);
            Clone.getPagesThatBelongToMe().add(Integer.valueOf(firstPageForTable.getPageName()));
            firstPageForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), Clone);


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

                for (ArrayList<Object> row : headPage.getPageRecords()) {
                    newRecs.add(new ArrayList<>(row));
                }

                // adding new value


                newRecs.forEach(row -> row.remove(attrIndex));

                // reinsert into new table

                for (ArrayList<Object> tempRec : newRecs) {
                    StorageManager.getStorageManager().insertRecord(Clone, tempRec);
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
            System.err.println("failure removing attribute from table " + table.getTableName());
            return false;
        }
    }


    public BPlusTree newIndex(Table table, BPlusTree bpTree, int attributeIdx) {

        // page name for head is always at idx zero
        int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

        // where in a row the pk is
        int pkidx = ((Table) table).pkIdx();


        // loop though all the tables pages in order
        int idx = 0;

        while (headPtr != -1) {

            Page headPage = pb.getPageFromBuffer("" + headPtr, table);
            // look though all record for that page

            idx = 0;
            for (ArrayList<Object> row : headPage.getPageRecords()) {
                RecordPointer rp = new RecordPointer(headPtr, idx);


                switch (bpTree.Type) {
                    case "integer":
                        bpTree.insertRecordPointer(rp, (Integer) row.get(attributeIdx));
                        break;
                    case "double":
                        bpTree.insertRecordPointer(rp, (Double) row.get(attributeIdx));
                        break;
                    case "boolean":
                        bpTree.insertRecordPointer(rp, (Boolean) row.get(attributeIdx));
                        break;
                    default:
                        bpTree.insertRecordPointer(rp, (String) row.get(attributeIdx));
                        break;
                }

                idx++;

            }
            // next page
            headPtr = headPage.getPtrToNextPage();
        }
        return bpTree;
    }

    public PageBuffer getPb() {
        return pb;
    }

}
