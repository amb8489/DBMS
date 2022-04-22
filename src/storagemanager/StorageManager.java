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

    // DONE

    @Override
    public ArrayList<Object> getRecord(ITable table, Object pkValue) {

        // getting the pk index tree

        BPlusTree tree = ((Table) table).getPkTree();

        // searching the tree
        ArrayList<RecordPointer> rps = switch (tree.Type) {
            case "integer" -> tree.search((Integer) pkValue);
            case "double" -> tree.search((Double) pkValue);
            case "boolean" -> tree.search((Boolean) pkValue);
            default -> tree.search((String) pkValue);
        };
        // getting the page
        int pageName = rps.get(0).page();
        int idxInPage = rps.get(0).index();
        Page page = pb.getPageFromBuffer(String.valueOf(pageName), table);

        // getting the record in the page
        return page.getPageRecords().get(idxInPage);

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

    // DONE
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


            ////////////////////// pre preprocessing before inset //////////////////////

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

            //TODO why is this happening here???--------------------------------------------

            // checking if fk in other table will need to optimize search
            for (ForeignKey fk : (table).getForeignKeys()) {
                String fkAttribute = fk.getAttrName();
                Object valueToFindInFKtab = record.get(AttribNamesIdx.get(fkAttribute));

                if (!TableContainsFkVal(fk, valueToFindInFKtab)) {
                    System.err.println(valueToFindInFKtab + " not in fk " + fk);

                    return false;
                }

            }

            ////////////////////// inserting phase //////////////////////

            //TODO deny dups

            BPlusTree tree = ((Table) table).getPkTree();

            // searching where in the tree this would go ????
            var pkValue = record.get(((Table) table).pkIdx());
            //To finish b plus tree, we need to make sure the record doesnt already exist in a tree.
            //Search for the record in the tree, if it does exist, error with primary key should be unique
            int listSize = switch (tree.Type) {
                case "integer" -> tree.search((Integer) pkValue).size();
                case "double" -> tree.search((Double) pkValue).size();
                case "boolean" -> tree.search((Boolean) pkValue).size();
                default -> tree.search((String) pkValue).size();
            };
            if (listSize > 0) {
                System.err.println("ERROR Primary Key already exists in tree");
                return false;
            }


            RecordPointer rp = switch (tree.Type) {
                case "integer" -> tree.findInserPostion((Integer) pkValue);
                case "double" -> tree.findInserPostion((Double) pkValue);
                case "boolean" -> tree.findInserPostion((Boolean) pkValue);
                default -> tree.findInserPostion((String) pkValue);
            };


//            // insering into page getting the page
            int pageName = rp.page();
            int idxInPage = rp.index();

            // -1 means inset into that tables first page at index 0
            if (pageName < 0) {
                pageName = ((Table) table).getPagesThatBelongToMe().get(0);
                rp = new RecordPointer(pageName, idxInPage);
            }

            // insert rp & update indexes for tht page

            switch (tree.Type) {
                case "integer" -> tree.insertRecordPointer(rp, (Integer) pkValue);
                case "double" -> tree.insertRecordPointer(rp, (Double) pkValue);
                case "boolean" -> tree.insertRecordPointer(rp, (Boolean) pkValue);
                default -> tree.insertRecordPointer(rp, (String) pkValue);
            }
            ;


            // get the page to insert to
            Page page = pb.getPageFromBuffer(String.valueOf(rp.page()), table);

            // inserting to actual page
            // error comming from not updating records in tree after a page split
            page.insert(rp.index(), record);


            return true;


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
        ////////////////////// deleteRecord phase //////////////////////

        try {

            BPlusTree tree = ((Table) table).getPkTree();

            // searching where in the tree where value is
            ArrayList<RecordPointer> rp = switch (tree.Type) {
                case "integer" -> tree.search((Integer) primaryKey);
                case "double" -> tree.search((Double) primaryKey);
                case "boolean" -> tree.search((Boolean) primaryKey);
                default -> tree.search((String) primaryKey);
            };


            // value did not exist in table
            if (rp.size() == 0) {
                System.out.println("couldnt find:" + primaryKey);
                return false;
            }


            RecordPointer deleteLocation = rp.get(0);


            // delete rp & TODO update indexes for thatt page in tree -1 from each idx

            switch (tree.Type) {
                case "integer" -> tree.removeRecordPointer(deleteLocation, (Integer) primaryKey);
                case "double" -> tree.removeRecordPointer(deleteLocation, (Double) primaryKey);
                case "boolean" -> tree.removeRecordPointer(deleteLocation, (Boolean) primaryKey);
                default -> tree.removeRecordPointer(deleteLocation, (String) primaryKey);
            }


            // get the page to delete from
            Page page = pb.getPageFromBuffer(String.valueOf(deleteLocation.page()), table);

            return page.delete(deleteLocation.index());


        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
            System.err.println("Storage manager(insertRecord): error in deleing");
            return false;
        }

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
