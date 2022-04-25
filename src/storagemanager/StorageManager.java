/*
Kyle Ferguson, Aaron Berghash, Emma Reynolds
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


/*
//////////////////// TODO ////////////////////
  - restore tree when restoring DB
/////////////////////////////////////////////
*/
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

    public boolean clearTablesPages(ITable table) {
        if (table instanceof Table workingTable) {  //cool piece of code IntelliJ made for me.
            // workingTable is a "patter var" https://openjdk.java.net/jeps/394
            ArrayList<Integer> tablePages = workingTable.getPagesThatBelongToMe();
            for (int page : tablePages) {
                FileSystem.deletePageFile(page);
            }

            return true;
        }

        return false;
    }

    public boolean DeletePageFromTable(ITable table, int pageName) {
        if (table instanceof Table workingTable) {  //cool piece of code IntelliJ made for me.
            // workingTable is a "patter var" https://openjdk.java.net/jeps/394
            ArrayList<Integer> tablePages = workingTable.getPagesThatBelongToMe();
            if (tablePages.contains(pageName)) {
                FileSystem.deletePageFile(pageName);
                return true;
            }
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
        if (rps.size() <= 0) {
            System.err.println("Record Pointer does not exist: Cannot get Record");
            return null;
        }
        // getting the page
        int pageName = rps.get(0).page();
        int idxInPage = rps.get(0).index();
        Page page = pb.getPageFromBuffer(String.valueOf(pageName), table);

        // getting the record in the page
        return page.getPageRecords().get(idxInPage);

    }

    // TODO WITH Tree
    public boolean TableContainsFkVal(ForeignKey fk, Object wanted) {
        try {
            Table FkTable = (Table) Catalog.getCatalog().getTable(fk.getRefTableName());

            // page name for head is always at idx zero
            int headPtr = (FkTable).getPagesThatBelongToMe().get(0);

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
                // look through all record for that page
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

    // done
    @Override
    public boolean insertRecord(ITable table, ArrayList<Object> record) {
        try {


            ////////////////////// pre preprocessing before inset //////////////////////


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
                        // removing " before placing in db
                        record.set(idxx, str.replace("\"", ""));
                    }
                }
                idxx++;
            }

            // looking for inserting null when it shouldn't be
            for (Integer i : ((Table) table).indicesOfNotNullAttributes) {
                if (record.get(i) == null) {
                    System.err.println("attribute: " + table.getAttributes().get(i).getAttributeName() + " cant be null");
                    return false;
                }
            }


            HashMap<String, Integer> AttribNamesIdx = ((Table) table).AttribIdxs;
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


            BPlusTree tree = ((Table) table).getPkTree();

            // searching where in the tree this would go ????
            var pkValue = record.get(((Table) table).pkIdx());

            //To finish b plus tree, we need to make sure the record doesn't already exist in a tree.
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

            // finding where we would insert the new rec
            RecordPointer rp = switch (tree.Type) {
                case "integer" -> tree.findInserPostion((Integer) pkValue);
                case "double" -> tree.findInserPostion((Double) pkValue);
                case "boolean" -> tree.findInserPostion((Boolean) pkValue);
                default -> tree.findInserPostion((String) pkValue);
            };


//            // inserting into page getting the page
            int pageName = rp.page();
            int idxInPage = rp.index();

            // -1 means inset into that tables first page at index 0
            if (pageName < 0) {
                pageName = ((Table) table).getPagesThatBelongToMe().get(0);
                rp = new RecordPointer(pageName, idxInPage);
            }

            // insert rp & update indexes for each index on page
            for (String name : ((Table) table).IndexedAttributes.keySet()) {

                tree = ((Table) table).IndexedAttributes.get(name);

                var inValue = record.get(((Table) table).AttribIdxs.get(name));
                switch (tree.Type) {
                    case "integer" -> tree.insertRecordPointer(rp, (Integer) inValue);
                    case "double" -> tree.insertRecordPointer(rp, (Double) inValue);
                    case "boolean" -> tree.insertRecordPointer(rp, (Boolean) inValue);
                    default -> tree.insertRecordPointer(rp, (String) inValue);
                }
            }


            // get the page to insert to
            Page page = pb.getPageFromBuffer(String.valueOf(rp.page()), table);

            // inserting to actual page
            // error coming from not updating records in tree after a page split
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
        String whereStmt = where;
        // 1) find what attributes in the where statement are indexed
        var table1 = ((Table) table);

        // 1a) parse where statement in to tokens

        whereStmt = whereStmt.replace("(", " ( ");
        whereStmt = whereStmt.replace(")", " ) ");

        whereStmt = whereStmt.replace("!", " !");
        whereStmt = whereStmt.replace("<", " < ");
        whereStmt = whereStmt.replace(">", " > ");
        whereStmt = whereStmt.replace("=", " = ");

        whereStmt = whereStmt.replace("<  =", " <= ");
        whereStmt = whereStmt.replace(">  =", " >= ");
        whereStmt = whereStmt.replace("! =", " != ");
        whereStmt = whereStmt.replace(" .", ".");
        whereStmt = whereStmt.replace(". ", ".");

        // tokenize the string by spaces
        List<String> tokens = Utilities.mkTokensFromStr(whereStmt);


        // getting tokens after the where
        int whereIdx = 1;
        for (String t : tokens) {
            if (t.equalsIgnoreCase("where")) {
                tokens = tokens.subList(whereIdx, tokens.size());
                break;
            }
            whereIdx++;
        }


        //1b) find what columns if any have indices that can be used to reduce the search space

        ArrayList<String> indexedColsInWhere = new ArrayList<>();
        for (String token : tokens) {
            if (((Table) table).IndexedAttributes.containsKey(token)) {
                indexedColsInWhere.add(token);
            }
        }


        // we have indices that we can work with
        if (!indexedColsInWhere.isEmpty()) {


            // 2) get set of possible usable recs or at least the pages where those recs live
            // so that we don't need to load up the entire table in mem at once

            // 2a) make set of possible acceptable records

            // loop though indexed attribute names and get the index tree to work with

            // store of possible places to look

            // 3) see if indices can even be useful in this where statement


            HashSet<String> operators = new HashSet<>(List.of(new String[]{"=", ">", ">=", "<", "<=", "!="}));

            //==========================================================================================
            //===================================== case 1 =============================================
            //=================================== col op value =========================================

            //  indexed operator value / value operator indexed;
            // ex: bar = 5 if bar has an index or 5 = bar


            if (tokens.size() == 3 && (indexedColsInWhere.size() == 1)) {

                // list of final records
                ArrayList<RecordPointer> rps = new ArrayList<>();

                for (String attributeName : indexedColsInWhere) {
                    // get the tree for that attribute
                    var currTree = ((Table) table).IndexedAttributes.get(attributeName);

                    // find the operator should be in the middle
                    String operator = null;
                    int idx = 0;
                    for (String token : tokens) {
                        if (operators.contains(token)) {
                            operator = token;
                            break;
                        }
                        idx++;
                    }

                    // operator should exist and should be in the middle
                    if (operator == null || idx != 1) {
                        System.err.println("error in where statement missing/ badly placed operator");
                        return false;
                    }

                    // find the value and index
                    String attrib = tokens.get(0);
                    String value = tokens.get(2);

                    if (indexedColsInWhere.contains(tokens.get(2))) {
                        attrib = tokens.get(2);
                        value = tokens.get(0);
                    }


                    rps = GetRecsFromTreeWhere(currTree, operator, value);

                }
                // for case all that's left is just to get rps FROM TABLE
                // make new table and just add these values to it

                Collections.reverse(rps);
                for (RecordPointer rp : rps) {
                    Page page = pb.getPageFromBuffer(String.valueOf(rp.page()), table);
                    var row = page.getPageRecords().get(rp.index());
                    System.out.println("success----> deleting :" + row.get(((Table) table).pkIdx()) + " " + deleteRecord(table, row.get(((Table) table).pkIdx())));
                    System.out.println("-------------------delete where HERE------------------------" + rps.size());
                    ((Table) table).getPkTree().printRPS();
                }

                // because simple case we won't need to check were for correctness it's implicit from the tree
                return true;

            }


            //==========================================================================================
            //===================================== case 2 =============================================
            //================ multiple case1 chained by AND and ors  ===============


            // case three much more complicated
            // we can have or if they have an index, we don't have one then
            // we have no choice but to brute force it
            // we can have a mix of ands and ors iff the ands have at least one idx and the ors also have and index
            // CHAIN of OR and AND where ALL needed cases have an index
            // over hea night not be with it in larger cases
            // add recs to hash set to reduce dubs
            // case1 or case1 { or case1... cas1}
            boolean isLegalCase3 = WhereP3.isLegalCase2(tokens, (Table) table);

            if (isLegalCase3) {
//                System.err.println("isLegalCase3 __>" + isLegalCase3);

                ArrayList<RecordPointer> rps = WhereP3.GetCase2Recs(tokens, (Table) table);

                ArrayList<ArrayList<Object>> found = new ArrayList<>();

                for (var rp : rps) {
                    int pageName = rp.page();
                    int pageidx = rp.index();
                    Page p = pb.getPageFromBuffer(String.valueOf(pageName), table);

                    var row = p.getPageRecords().get(pageidx);

                    if (wp.whereIsTrue(whereStmt, (Table) table, row)) {
                        deleteRecord(table, row.get(((Table) table).pkIdx()));
                    }

                }
                return true;
            }
        }
        System.out.println("INDEX NOT USEFUL IN THIS WHERE STATEMENT");
        // no useful index operation old style
        ArrayList<ArrayList<Object>> found = new ArrayList<>();

        try {


            // page name for head is always at idx zero
            int headPtr = ((Table) table).getPagesThatBelongToMe().get(0);

            ArrayList<Attribute> attributes = table.getAttributes();
            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);
                // look through all record for that page

                int recSize = headPage.getPageRecords().size();

                for (int i = recSize - 1; i > -1; i--) {
                    ArrayList<Object> row = headPage.getPageRecords().get(i);
                    if (wp.whereIsTrue(whereStmt, (Table) table, row)) {

                        deleteRecord(table, row.get(((Table) table).pkIdx()));

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


    // done
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
                System.out.println("Couldn't find: " + primaryKey + " in table");
                return false;
            }


            RecordPointer deleteLocation = rp.get(0);


            // delete rp & update indexes for that page in tree -1 from each idx


            switch (tree.Type) {
                case "integer" -> tree.removeRecordPointer(deleteLocation, (Integer) primaryKey);
                case "double" -> tree.removeRecordPointer(deleteLocation, (Double) primaryKey);
                case "boolean" -> tree.removeRecordPointer(deleteLocation, (Boolean) primaryKey);
                default -> tree.removeRecordPointer(deleteLocation, (String) primaryKey);
            }


            // get the page to delete from
            Page page = pb.getPageFromBuffer(String.valueOf(deleteLocation.page()), table);

            var DeleteRecord = page.getPageRecords().get(deleteLocation.index());


            // get the row tht was deleted and update the other indexed tables

            String pkAttributeNAme = table.getAttributes().get(((Table) table).pkIdx()).getAttributeName();
            for (String name : ((Table) table).IndexedAttributes.keySet()) {

                if (!name.equals(pkAttributeNAme)) {

                    tree = ((Table) table).IndexedAttributes.get(name);

                    // grabbing the value in the row that belongs to that attribute
                    var inValue = DeleteRecord.get(((Table) table).AttribIdxs.get(name));
                    switch (tree.Type) {
                        case "integer" -> tree.removeRecordPointer(deleteLocation, (Integer) inValue);
                        case "double" -> tree.removeRecordPointer(deleteLocation, (Double) inValue);
                        case "boolean" -> tree.removeRecordPointer(deleteLocation, (Boolean) inValue);
                        default -> tree.removeRecordPointer(deleteLocation, (String) inValue);
                    }
                }
            }


            return page.delete(deleteLocation.index());


        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
            System.err.println("Storage manager(insertRecord): error in deleting");
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

    //DONE
    @Override
    public boolean updateRecord(ITable table, ArrayList<Object> oldRecord, ArrayList<Object> newRecord) {

        // search for oldRecord to make sure it's in the tree , if not then return false
        // we can't update something that doesn't exist

        BPlusTree tree = ((Table) table).getPkTree();

        var pkValue = oldRecord.get(((Table) table).pkIdx());

        // finding how many of these exist in the table
        ArrayList<RecordPointer> rps = switch (tree.Type) {
            case "integer" -> tree.search((Integer) pkValue);
            case "double" -> tree.search((Double) pkValue);
            case "boolean" -> tree.search((Boolean) pkValue);
            default -> tree.search((String) pkValue);
        };
        // if non exist then error
        if (rps.size() == 0) {
            System.err.println("record to update " + oldRecord + " does not exist in the table");
            return false;
        }


        // delete delete old rec

        deleteRecord(table, oldRecord.get(((Table) table).pkIdx()));

        // if we can insert it meaning no dup pk then insert new rec

        boolean successfulInsert = insertRecord(table, newRecord);

        // if new rec has dup pk then re-add the old rec
        // restoring after bad insert
        if (!successfulInsert) {
            insertRecord(table, oldRecord);
        }

        return successfulInsert;

    }

    @Override
    public void purgePageBuffer() {
        pb.PurgeBuffer();
    }


    // done
    @Override
    public boolean addAttributeValue(ITable table, Object defaultValue) {


        try {


            // get the pages that belong to this table
            ArrayList<Integer> pages = ((Table) table).getPagesThatBelongToMe();

            // get the first table in the table
            int headPtr = pages.get(0);

            // clone the old table

            Table Clone = new Table(table);


            // clear the clone table pages (we will make new ones)
            Clone.getPagesThatBelongToMe().clear();

            // make a new first page for the clone table and add it to clone table
            Page firstPageForTable = new Page(Clone);


            firstPageForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), Clone);

            //tree for tHIS TABLE  new empty table
            var newTree = BPlusTree.TreeFromTableAttribute(Clone, Clone.pkIdx());

            Clone.IndexedAttributes.put(table.getAttributes().get(Clone.pkIdx()).getAttributeName(), newTree);


            // remove the new attribute because we need the old schema to read in the old table pages
            table.getAttributes().remove(table.getAttributes().size() - 1);

            // looping though all the pages of the old table
            while (headPtr != -1) {


                // load the page from memory
                Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);


                // cloning old table page records
                ArrayList<ArrayList<Object>> newRecs = new ArrayList<>();
                for (ArrayList<Object> row : headPage.getPageRecords()) {
                    newRecs.add(new ArrayList<>(row));
                }

                // adding new attribute/value to each row
                // these will be the clone table new records

                newRecs.forEach(row -> row.add(defaultValue));


                // reinsert new recs into new table with the updated schema
                for (ArrayList<Object> tempRec : newRecs) {
                    StorageManager.getStorageManager().insertRecord(Clone, tempRec);
                }


                // getting next page from old table

                // mem optimization remove the old page from disk
                // and not wait till end to remove all the page that way we would only ever have one
                // page duplicate vs an entire table duped in memory at the end
                DeletePageFromTable(table, headPtr);
                headPtr = headPage.getPtrToNextPage();

            }

            clearTablesPages(table);
            // set new table name to old table

            ((Table) table).SetAttributes(Clone.getAttributes());
            ((Table) table).SetIndicesOfNotNullAttributes(Clone.indicesOfNotNullAttributes);
            ((Table) table).SetPagesThatBelongToMe(Clone.getPagesThatBelongToMe());
            ((Table) table).SetAttribIdxs(Clone.AttribIdxs);
            ((Table) table).IndexedAttributes = Clone.IndexedAttributes;

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("failure adding attribute from table " + table.getTableName());
            return false;
        }
    }


    //done
    @Override
    public boolean dropAttributeValue(ITable table, int attrIndex) {


        try {
            if (attrIndex >= table.getAttributes().size()) {
                System.err.println("Table cant remove attribute at index " + attrIndex + " because" +
                        " table doesn't have that many attributes");
                return false;
            }
            if (attrIndex == ((Table) table).pkIdx()) {
                System.err.println("ERROR: trying to drop the primary key");
                return false;
            }


            // get the pages that belong to this table
            ArrayList<Integer> pages = ((Table) table).getPagesThatBelongToMe();

            // get the first table in the table
            int headPtr = pages.get(0);

            // clone the old table

            Table Clone = new Table(table);

            // remove the attribute because we need the old schema to read in the old table pages
            Clone.dropAttribute(table.getAttributes().get(attrIndex).getAttributeName());


            // clear the clone table pages (we will make new ones)
            Clone.getPagesThatBelongToMe().clear();

            // make a new first page for the clone table and add it to clone table
            Page firstPageForTable = new Page(Clone);


            firstPageForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), Clone);

            //tree for tHIS TABLE  new empty table
            var newTree = BPlusTree.TreeFromTableAttribute(Clone, Clone.pkIdx());

            Clone.IndexedAttributes.put(table.getAttributes().get(Clone.pkIdx()).getAttributeName(), newTree);


            // looping though all the pages of the old table
            while (headPtr != -1) {


                // load the page from memory
                Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);


                // cloning old table page records
                ArrayList<ArrayList<Object>> newRecs = new ArrayList<>();
                for (ArrayList<Object> row : headPage.getPageRecords()) {
                    newRecs.add(new ArrayList<>(row));
                }

                // adding new attribute/value to each row
                // these will be the clone table new records

                newRecs.forEach(row -> row.remove(attrIndex));


                // reinsert new recs into new table with the updated schema
                for (ArrayList<Object> tempRec : newRecs) {
                    StorageManager.getStorageManager().insertRecord(Clone, tempRec);
                }


                // getting next page from old table

                // mem optimization remove the old page from disk
                // and not wait till end to remove all the page that way we would only ever have one
                // page duplicate vs an entire table duped in memory at the end

                DeletePageFromTable(table, headPtr);

                headPtr = headPage.getPtrToNextPage();

            }

            clearTablesPages(table);
            // set new table name to old table

            ((Table) table).SetAttributes(Clone.getAttributes());
            ((Table) table).SetIndicesOfNotNullAttributes(Clone.indicesOfNotNullAttributes);
            ((Table) table).SetPagesThatBelongToMe(Clone.getPagesThatBelongToMe());
            ((Table) table).SetAttribIdxs(Clone.AttribIdxs);
            ((Table) table).IndexedAttributes = Clone.IndexedAttributes;

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("failure adding attribute from table " + table.getTableName());
            return false;
        }
    }


    public BPlusTree newIndex(Table table, BPlusTree bpTree, int attributeIdx) {

        // page name for head is always at idx zero
        int headPtr = table.getPagesThatBelongToMe().get(0);


        // loop though all the tables pages in order
        int idx;


        while (headPtr != -1) {

            Page headPage = pb.getPageFromBuffer(String.valueOf(headPtr), table);
            // look through all record for that page

            idx = 0;
            for (ArrayList<Object> row : headPage.getPageRecords()) {
                RecordPointer rp = new RecordPointer(headPtr, idx);


                switch (bpTree.Type) {
                    case "integer" -> bpTree.insertRecordPointer(rp, (Integer) row.get(attributeIdx));
                    case "double" -> bpTree.insertRecordPointer(rp, (Double) row.get(attributeIdx));
                    case "boolean" -> bpTree.insertRecordPointer(rp, (Boolean) row.get(attributeIdx));
                    default -> bpTree.insertRecordPointer(rp, (String) row.get(attributeIdx));
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

    public ArrayList<ArrayList<Object>> getWhere(Table table, String whereStmt) {


        // 1) find what attributes in the where statement are indexed
        var table1 = ((Table) table);
        System.out.println("indices on :" + table1.IndexedAttributes.keySet());

        // 1a) parse where statement in to tokens

        whereStmt = whereStmt.replace("(", " ( ");
        whereStmt = whereStmt.replace(")", " ) ");

        whereStmt = whereStmt.replace("!", " !");
        whereStmt = whereStmt.replace("<", " < ");
        whereStmt = whereStmt.replace(">", " > ");
        whereStmt = whereStmt.replace("=", " = ");

        whereStmt = whereStmt.replace("<  =", " <= ");
        whereStmt = whereStmt.replace(">  =", " >= ");
        whereStmt = whereStmt.replace("! =", " != ");
        whereStmt = whereStmt.replace(" .", ".");
        whereStmt = whereStmt.replace(". ", ".");

        // tokenize the string by spaces
        List<String> tokens = Utilities.mkTokensFromStr(whereStmt);


        // getting tokens after the where
        int whereIdx = 1;
        for (String t : tokens) {
            if (t.equalsIgnoreCase("where")) {
                tokens = tokens.subList(whereIdx, tokens.size());
                break;
            }
            whereIdx++;
        }


        //1b) find what columns if any have indices that can be used to reduce the search space

        ArrayList<String> indexedColsInWhere = new ArrayList<>();
        for (String token : tokens) {
            if ((table).IndexedAttributes.containsKey(token)) {
                indexedColsInWhere.add(token);
            }
        }


        // we have indices that we can work with
        if (!indexedColsInWhere.isEmpty()) {


            // 2) get set of possible usable recs or at least the pages where those recs live
            // so that we don't need to load up the entire table in mem at once

            // 2a) make set of possible acceptable records

            // loop though indexed attribute names and get the index tree to work with

            // store of possible places to look

            // 3) see if indices can even be useful in this where statement


            HashSet<String> operators = new HashSet<>(List.of(new String[]{"=", ">", ">=", "<", "<=", "!="}));

            //==========================================================================================
            //===================================== case 1 =============================================
            //=================================== col op value =========================================

            //  indexed operator value / value operator indexed;
            // ex: bar = 5 if bar has an index or 5 = bar


            if (tokens.size() == 3 && (indexedColsInWhere.size() == 1)) {

                // list of final records
                ArrayList<RecordPointer> rps = new ArrayList<>();

                for (String attributeName : indexedColsInWhere) {
                    // get the tree for that attribute
                    var currTree = (table).IndexedAttributes.get(attributeName);

                    // find the operator should be in the middle
                    String operator = null;
                    int idx = 0;
                    for (String token : tokens) {
                        if (operators.contains(token)) {
                            operator = token;
                            break;
                        }
                        idx++;
                    }

                    // operator should exist and should be in the middle
                    if (operator == null || idx != 1) {
                        System.err.println("error in where statement missing/ badly placed operator");
                        return null;
                    }

                    // find the value and index
                    String attrib = tokens.get(0);
                    String value = tokens.get(2);

                    if (indexedColsInWhere.contains(tokens.get(2))) {
                        attrib = tokens.get(2);
                        value = tokens.get(0);
                    }


                    rps = GetRecsFromTreeWhere(currTree, operator, value);

                }
                // for case all that's left is just to get rps FROM TABLE
                // make new table and just add these values to it
                ArrayList<ArrayList<Object>> found = new ArrayList<>();
                for (RecordPointer rp : rps) {
                    Page page = pb.getPageFromBuffer(String.valueOf(rp.page()), table);
                    var row = page.getPageRecords().get(rp.index());
                    found.add(row);
                }

                // because simple case we won't need to check were for correctness it's implicit from the tree
                return found;

            }


            //==========================================================================================
            //===================================== case 2 =============================================
            //================ multiple case1 chained by AND and ors  ===============


            // case three much more complicated
            // we can have or if they have an index, we don't have one then
            // we have no choice but to brute force it
            // we can have a mix of ands and ors iff the ands have at least one idx and the ors also have and index
            // CHAIN of OR and AND where ALL needed cases have an index
            // over hea night not be with it in larger cases
            // add recs to hash set to reduce dubs
            // case1 or case1 { or case1... cas1}
            boolean isLegalCase3 = WhereP3.isLegalCase2(tokens, table);

            if (isLegalCase3) {
//                System.err.println("isLegalCase3 __>" + isLegalCase3);

                ArrayList<RecordPointer> rps = WhereP3.GetCase2Recs(tokens, table);

                ArrayList<ArrayList<Object>> found = new ArrayList<>();

                for (var rp : rps) {
                    int pageName = rp.page();
                    int pageidx = rp.index();
                    Page p = pb.getPageFromBuffer(String.valueOf(pageName), table);

                    var row = p.getPageRecords().get(pageidx);

                    if (wp.whereIsTrue(whereStmt, table, row)) {
                        found.add(row);
                    }

                }
                return found;
            }


        }
        System.out.println("INDEX NOT USEFUL IN THIS WHERE STATEMENT");
        // no useful index operation old style
        ArrayList<ArrayList<Object>> found = new ArrayList<>();

        try {


            // page name for head is always at idx zero
            int headPtr = (table).getPagesThatBelongToMe().get(0);

            ArrayList<Attribute> attributes = table.getAttributes();
            // loop though all the tables pages in order
            while (headPtr != -1) {

                Page headPage = pb.getPageFromBuffer("" + headPtr, table);
                // look through all record for that page

                int recSize = headPage.getPageRecords().size();

                for (int i = recSize - 1; i > -1; i--) {
                    ArrayList<Object> row = headPage.getPageRecords().get(i);
                    if (wp.whereIsTrue(whereStmt, table, row)) {
                        found.add(row);
                    }
                }

                // next page
                headPtr = headPage.getPtrToNextPage();
            }
            return found;
        } catch (Exception e) {
            System.err.println("error removing in remove where in sm");
            return new ArrayList<>();
        }
    }

    public static ArrayList<RecordPointer> GetRecsFromTreeWhere(BPlusTree currTree, String operator, String value) {
        // finding records     yes... i know


        ArrayList<RecordPointer> rps;

        switch (currTree.Type) {
            case "integer" -> {
                int searchKey = Integer.parseInt(value);

                rps = switch (operator) {
                    case "=" -> currTree.search(searchKey);
                    case ">" -> currTree.searchRange(searchKey, false, false);
                    case ">=" -> currTree.searchRange(searchKey, false, true);
                    case "<" -> currTree.searchRange(searchKey, true, false);
                    case "<=" -> currTree.searchRange(searchKey, true, true);
                    // !=
                    default -> currTree.searchNotEq(searchKey);

                };
            }

            case "double" -> {
                double searchKey = Double.parseDouble(value);

                rps = switch (operator) {
                    case "=" -> currTree.search(searchKey);
                    case ">" -> currTree.searchRange(searchKey, false, false);
                    case ">=" -> currTree.searchRange(searchKey, false, true);
                    case "<" -> currTree.searchRange(searchKey, true, false);
                    case "<=" -> currTree.searchRange(searchKey, true, true);
                    // !=
                    default -> currTree.searchNotEq(searchKey);
                };
            }
            case "boolean" -> {
                boolean searchKey = Boolean.parseBoolean(value);

                rps = switch (operator) {
                    case "=" -> currTree.search(searchKey);
                    case ">" -> currTree.searchRange(searchKey, false, false);
                    case ">=" -> currTree.searchRange(searchKey, false, true);
                    case "<" -> currTree.searchRange(searchKey, true, false);
                    case "<=" -> currTree.searchRange(searchKey, true, true);
                    // !=
                    default -> currTree.searchNotEq(searchKey);
                };
            }
            default -> {
                String searchKey = value;

                rps = switch (operator) {
                    case "=" -> currTree.search(searchKey);
                    case ">" -> currTree.searchRange(searchKey, false, false);
                    case ">=" -> currTree.searchRange(searchKey, false, true);
                    case "<" -> currTree.searchRange(searchKey, true, false);
                    case "<=" -> currTree.searchRange(searchKey, true, true);
                    // !=
                    default -> currTree.searchNotEq(searchKey);
                };
            }

        }
        return rps;
    }
}
