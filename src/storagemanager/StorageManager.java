/*
Kyle Ferguson, Aaron Berghash
 */

package storagemanager;

import catalog.Catalog;
import common.*;
import pagebuffer.PageBuffer;

import java.util.ArrayList;
import java.util.HashMap;

import filesystem.FileSystem;
import parsers.WhereParser;

public class StorageManager extends AStorageManager {


    private static PageBuffer pb;
    private static WhereParser wp;

    public StorageManager() {

        pb = new PageBuffer(Catalog.getCatalog().getPageBufferSize());
        wp = new WhereParser();
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
            for (Attribute a:  FkTable.getAttributes()){
                if (a.getAttributeName().equals(fk.refAttribute())){
                    break;
                }else{
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


        }catch (Exception e){
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
        try {


            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);
//        VerbosePrint.print("head page: "+headPtr);
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
                    System.err.println(valueToFindInFKtab+" not in fk "+fk);

                    return false;
                }

            }

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


                    int pkid = ((Table) table).pkIdx();
                    String pk_type = table.getAttributes().get(pkid).getAttributeType().toUpperCase();

                    switch (pk_type.charAt(0)) {
                        case 'I' -> {
                            int resI = ((Integer) record.get(pkidx)).compareTo((Integer) row.get(pkidx));
                            if (resI == 0) {
                                return false;
                            }

                            if (resI < 0) {

                                headPage.getPageRecords().add(idx, record);
                                headPage.wasChanged = true;
                                return true;
                            }
                        }
                        case 'D' -> {
                            int resD = ((Double) record.get(pkidx)).compareTo((Double) row.get(pkidx));

                            if (resD == 0) {
                                return false;
                            }
                            if (resD < 0) {
                                headPage.getPageRecords().add(idx, record);
                                headPage.wasChanged = true;
                                return true;
                            }
                        }
                        case 'B' -> {
                            int resB = ((Boolean) record.get(pkidx)).compareTo((Boolean) row.get(pkidx));
                            if (resB == 0) {
                                return false;
                            }
                            if (resB < 0) {
                                headPage.getPageRecords().add(idx, record);
                                headPage.wasChanged = true;
                                return true;
                            }
                        }
                        default -> {
                            int resS = ((record.get(pkidx).toString()).compareTo(row.get(pkidx).toString()));

                            if (resS == 0) {
                                return false;
                            }
                            if (resS < 0) {


                                headPage.getPageRecords().add(idx, record);
                                headPage.wasChanged = true;
                                return true;
                            }
                        }
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
        } catch (Exception e) {
            System.err.println("Storage manager(insertRecord): error in inserting");
            return false;
        }
    }

    //TODO testing
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
                    if (removeAllRecords || wp.whereIsTrue(where, row, attributes)) {
                        VerbosePrint.Verbose = true;
                        VerbosePrint.print("REMOVING" + row);
                        VerbosePrint.Verbose = false;

                        headPage.getPageRecords().remove(i);
                        headPage.wasChanged = true;
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

            Page headPage = pb.getPageFromBuffer("" + headPtr, table);
            // look though all record for that page
            int idx = 0;
            for (ArrayList<Object> row : headPage.getPageRecords()) {

                if (row.get(pkidx).equals(primaryKey)) {
                    VerbosePrint.print("REMOVING" + row);

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


    //TODO testing
    // add val to the end of each row in the table
    @Override
    public boolean addAttributeValue(ITable table, Object defaultValue) {

        try {


            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);

                // for each row in page records append defult val
                headPage.getPageRecords().forEach(row -> row.add(defaultValue));
                headPage.wasChanged = true;
                // next page
                headPtr = headPage.getPtrToNextPage();
            }


            return true;
        } catch (Exception e) {
            System.err.println("failure adding attribute from table " + table.getTableName());
            return false;
        }
    }

    //TODO
    // - TEST reading and writing from updated page
    // - test removing out of bounds of attrib array

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

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);
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
