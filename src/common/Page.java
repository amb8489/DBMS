/*
 * Authors: Aaron Beghash, Kyle Ferguson
 */

package common;

import catalog.ACatalog;
import catalog.Catalog;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import filesystem.FileSystem;
import pagebuffer.PageBuffer;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;


/*****************************--page--*****************************

 A Class to represent a page in taken from memory
 *****************************************************************/

public class Page {

    // number of pages in DB and also used as the name of the page
    public static int numPages = 0;

    // was the record changed from being loaded from mem aka
    // inserted deleted split updated
    public boolean wasChanged = false;

    // -like a ptr in a linked list
    // ptr to the next page as an int that represents the name
    // of the next page that this page points to
    // -- negative number means the page points to null
    private int ptrToNextPage;

    // name of the page aka string of what # page this is
    private final int pageName;

    //current page size in bytes
    public int currentSize;

    // list of records that the page holds
    private List<ArrayList<Object>> pageRecords = new ArrayList<>();

    // the table that this page belongs to
    public ITable IBelongTo;

    // ****************************--constructors--*****************************

    // use this to make the first new page for a table
    // this page will be the root of the linked list of pages
    public Page(ITable iBelongTo) {
        numPages++;
        // new page has no size
        this.currentSize = 0;
        this.pageName = numPages;
        // first page points to nothing
        this.ptrToNextPage = -1;
        this.IBelongTo = iBelongTo;
        // adding this page to the table
        ((Table) this.IBelongTo).addPageAffiliations(numPages);

    }

    // USED WHEN LOADING A PAGE IN FROM MEM
    public Page(int pageName, int ptrToNextPage, int currentSize, List<ArrayList<Object>> pageRecords, ITable iBelongTo) {
        // is this a new page or just a re loaded page from mem
        this.IBelongTo = iBelongTo;


        if (!((Table) iBelongTo).getPagesThatBelongToMe().contains(pageName)) {
            numPages++;
        }

        // setting
        this.pageName = pageName;
        this.pageRecords = pageRecords;
        this.currentSize = currentSize;
        this.ptrToNextPage = ptrToNextPage;
    }


    // this will make a new page when splitting
    public Page(ITable iBelongTo, List<ArrayList<Object>> pageRecords) {
        // is this a new page or just a re loaded page from mem
        numPages++;


        // setting
        this.pageName = numPages;
        this.pageRecords = pageRecords;
        this.IBelongTo = iBelongTo;
        ((Table) this.IBelongTo).addPageAffiliations(numPages);

    }

    // ************************--getters | setter--*****************************


    public int getPtrToNextPage() {
        return ptrToNextPage;
    }

    public List<ArrayList<Object>> getPageRecords() {
        return pageRecords;
    }

    public String getPageName() {
        return String.valueOf(pageName);
    }

    public void setWasChanged(boolean wasChanged) {
        this.wasChanged = wasChanged;
    }

    public boolean isChanged() {
        return wasChanged;
    }



    // ****************************--methods--*****************************


    // loads into a page all the records and page info from disk
    // given the location of the page on disk and the table that the page belongs to;
    public static Page LoadFromDisk(String location, ITable table) {

        try {


            int charlen = 0;
            ArrayList<String> schema = new ArrayList<>();
            // looping though table attribs to get their types
            for (Attribute att : table.getAttributes()) {
                schema.add(att.getAttributeType());

                // found a char(#) paring for the number
                if (att.getAttributeType().startsWith("Char(")) {
                    charlen = Integer.parseInt(att.getAttributeType().substring(5, att.getAttributeType().length() - 1));
                }
            }

            /////////////////////////////////////////////////////////////////////////////

            VerbosePrint.print("reading records from page " + location);

            // read in streams
            DataInputStream dataInputStr = FileSystem.createPageDataInStream(location);

            // we will now start to calculate the page on read
            int currentSize = 0;

            // 1) reading page name
            int pageName = dataInputStr.readInt();
            currentSize += 4;


            // 2) read the num of records stored
            int numRecs = dataInputStr.readInt();
            currentSize += 4;

            //3) read pages ptr to the next page
            int ptrToNextPage = dataInputStr.readInt();
            currentSize += 4;


            // array list to hold page rows
            List<ArrayList<Object>> pageRecords = new ArrayList<>();

            //for each row in the page
            for (int rn = 0; rn < numRecs; rn++) {

                //------------------------ TODO WHAT IS GOING ON WITH THE BIT MASK FOR NULL VALS ------------------------

                // 4) read in int for bit array size (in bytes)
                int bitArraySizeInNumOfbytes = dataInputStr.readInt();
                currentSize += 4;


                // 5) read in bit array
                BitSet bitMask = BitSet.valueOf(dataInputStr.readNBytes(bitArraySizeInNumOfbytes));
                currentSize += bitArraySizeInNumOfbytes;



                // row obj this will rep 1 tuple in the table
                ArrayList<Object> rec = new ArrayList<>();

                //for each attribute in the tuple we use the schema to know how many bytes to read in
                // then store that in rec to make the row
                for (int idx = 0; idx < table.getAttributes().size(); idx++) {

                    // if bitmask is not a 1 (meaning value at that attribute is not null)
                    if (!bitMask.get(idx)) {

                        // 6) read in what the schema says is next
                        switch (schema.get(idx)) {

                            case "Integer":
                                System.out.println(schema+" "+table.getAttributes().size());
                                rec.add(dataInputStr.readInt());
                                currentSize += 4;
                                break;
                            case "Double":
                                rec.add(dataInputStr.readDouble());
                                currentSize += 8;
                                break;
                            case "Boolean":
                                rec.add(dataInputStr.readBoolean());
                                currentSize += 1;
                                break;
                            default:

                                // we get a char(#)
                                if (schema.get(idx).startsWith("Char(")) {
                                    rec.add(new String(dataInputStr.readNBytes(charlen), StandardCharsets.UTF_8));
                                    currentSize += (charlen);

                                } else {
                                    // var char should have an int before it telling how long the var char is
                                    // and how many bytes to read in
                                    int VarCharsize = dataInputStr.readInt();

                                    // then read in that many bytes
                                    rec.add(new String(dataInputStr.readNBytes(VarCharsize), StandardCharsets.UTF_8));
                                    currentSize += (VarCharsize) + 4;// 4 for the int we need to save for the varcahr
                                }
                        }
                    } else {
                        // if bit was a 1 we stored null in db
                        rec.add(null);
                    }
                }
                // append row to pageRecords
                pageRecords.add(rec);
            }
            VerbosePrint.print(pageName + " curr size " + currentSize);

            return new Page(pageName, ptrToNextPage, currentSize, pageRecords, table);

            // failure to find page or read fail
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("FAILURE TO READ IN PAGE: " + location);
            return null;
        }


    }


    // WRITE THE PAGE TO DISK
    public boolean writeToDisk(String location, ITable table) {
        try {

            // WHERE TO WRITE OUT PAGE TO
            location = Catalog.getCatalog().getDbLocation() + "/pages/" + this.pageName;


            ArrayList<String> schema = new ArrayList<>();
            for (Attribute att : table.getAttributes()) {
                schema.add(att.getAttributeType());
            }


            // output streams
            VerbosePrint.print(location);
            DataOutputStream out = FileSystem.createPageDataOutStream(String.valueOf(this.pageName));

            // byte array that we will store at the end(all the records stored as bytes at once to reduce the amount of
            // I/O operations)
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();


            // WRITE name ,num records to page, and name of next page (nullptr to next = -1)
            outputStream.write(ByteBuffer.allocate(4).putInt(Integer.parseInt(this.getPageName())).array());
            outputStream.write(ByteBuffer.allocate(4).putInt(this.pageRecords.size()).array());
            outputStream.write(ByteBuffer.allocate(4).putInt(this.ptrToNextPage).array());


            VerbosePrint.print("atemping store page to disk");

            //for each row in the table
            for (ArrayList<Object> record : this.pageRecords) {

                //for each attrib in row store to byte array
                // TODO
                //FIRST LETS TAKE CARE OF NULL VALS :)

                //------------------------ TODO WHAT IS GOING ON WITH THE BIT MASK FOR NULL VALS ------------------------

                int[] nullIndexes = IntStream.range(0, record.size()).filter(N -> record.get(N) == null).toArray();
                BitSet bitSet = new BitSet(record.size());
                for (int idx : nullIndexes) {
                    bitSet.set(idx);
                }

                byte[] nullMask = bitSet.toByteArray();

                // write out bitmask size int
                outputStream.write(ByteBuffer.allocate(4).putInt(nullMask.length).array());
                // write out mask
                outputStream.write(nullMask);
                //------------------------ ------------------------ ------------------------


                // make byte array from record
                // look though reach attribute and check the schema for its type and convert it to its bytes
                // and add it to outputStream btye array
                for (int idx = 0; idx < record.size(); idx++) {
                    if (record.get(idx) != null) {

                        System.out.println(schema+" "+record+" "+table.getTableName());
                        switch (schema.get(idx)) {
                            case "Integer":
                                //add it to outputStream btye array
                                outputStream.write(ByteBuffer.allocate(4).putInt((Integer) record.get(idx)).array());
                                break;
                            case "Double":
                                //add it to outputStream btye array

                                outputStream.write(ByteBuffer.allocate(8).putDouble((Double) record.get(idx)).array());
                                break;
                            case "Boolean":
                                //add it to outputStream btye array
                                outputStream.write(ByteBuffer.allocate(1).put(new byte[]{(byte) ((Boolean) record.get(idx) ? 1 : 0)}).array());
                                break;
                            default:
                                //add it to outputStream btye array
                                // char(#)
                                if (schema.get(idx).startsWith("Char(")) {
                                    outputStream.write(((String) record.get(idx)).getBytes());
                                } else {
                                    // add the len of var char before we write var char
                                    int VarCharlen = ((String) record.get(idx)).length();
                                    // write var char len (we need this to know how many bytes to write in when we read disk)
                                    outputStream.write(ByteBuffer.allocate(4).putInt(VarCharlen).array());
                                    // write var char
                                    outputStream.write(((String) record.get(idx)).getBytes());
                                }
                        }
                    }
                }
            }

            // all records added to byte array
            byte[] record_out = outputStream.toByteArray();

            // write out byte array to file
            out.write(record_out);
            out.close();

            currentSize = 0;
            VerbosePrint.print("Store complete");
            return true;


        } catch (IOException e) {
            System.err.println("COULD NOT Write FILE PAGE " + location);
            return false;
        }
    }


    public int calcPageSize(Page page) {

        ITable table = page.IBelongTo;
        List<ArrayList<Object>> recs = page.getPageRecords();
        // get schema from table that we need in order to know what type we are reading in
        // if record has a Char(#) type we need to know how long that char is so we know how many bytes to read in
        int charlen = 0;
        ArrayList<String> schema = new ArrayList<>();

        // looping though table attribs to get their types
        for (Attribute att : table.getAttributes()) {
            schema.add(att.getAttributeType());
            // found a char(#) paring for the number
            if (att.getAttributeType().startsWith("Char(")) {
                charlen = Integer.parseInt(att.getAttributeType().substring(5, att.getAttributeType().length() - 1));
            }
        }
        int size = 12;


        for (ArrayList<Object> r : recs) {

            //for each attrib in row store to byte array
            // make byte array from record
            // look though reach attribute and check the schema for its type and convert it to its bytes
            // and add it to outputStream btye array

            //TODO ??????
            size += 2;

            for (int idx = 0; idx < r.size(); idx++) {

                if (r.get(idx) != null) {
                    switch (schema.get(idx)) {
                        case "Integer":
                            size += 4;
                            break;
                        case "Double":
                            size += 8;
                            break;
                        case "Boolean":
                            size += 1;
                            break;
                        default:

                            // char(#)
                            if (schema.get(idx).startsWith("Char(")) {
                                size += charlen;
                            } else {
                                // add the len of var char before we write var char
                                int VarCharlen = ((String) r.get(idx)).length();
                                size += (4 + VarCharlen);
                            }
                    }
                }
            }
        }
        return size;

    }

    public int recordSize(ArrayList<Object> rec) {



        // get schema from table that we need in order to know what type we are reading in
        // if record has a Char(#) type we need to know how long that char is so we know how many bytes to read in
        int charlen = 0;
        ArrayList<String> schema = new ArrayList<>();
        // looping though table attribs to get their types
        for (Attribute att : this.IBelongTo.getAttributes()) {
            schema.add(att.getAttributeType());

            // found a char(#) paring for the number
            if (att.getAttributeType().startsWith("Char(")) {
                charlen = Integer.parseInt(att.getAttributeType().substring(5, att.getAttributeType().length() - 1));
            }
        }
        int size = 4;

        System.out.println(rec+"        "+schema);

        for (int idx = 0; idx < rec.size(); idx++) {

            if (rec.get(idx) != null) {
                switch (schema.get(idx)) {
                    case "Integer":
                        size += 4;
                        break;
                    case "Double":
                        size += 8;
                        break;
                    case "Boolean":
                        size += 1;
                        break;
                    default:

                        // char(#)
                        if (schema.get(idx).startsWith("Char(")) {
                            size += charlen;
                        } else {
                            // add the len of var char before we write var char
                            int VarCharlen = ((String) rec.get(idx)).length();
                            size += (4 + VarCharlen);
                        }
                }
            }
        }
        return size;
    }


    // will return the second half of the page
    public Page split() {


        // changed


        // split records in half
        int halfway = pageRecords.size() / 2;
        List<ArrayList<Object>> splitLeftHalf = new ArrayList<>(pageRecords.subList(0, halfway));
        List<ArrayList<Object>> splitRightHalf = new ArrayList<>(pageRecords.subList(halfway, pageRecords.size()));


        // assign new records
        this.pageRecords = splitLeftHalf;


        // make new page with split records
        Page splitPage = new Page(this.IBelongTo, splitRightHalf);


        // adjusting pointers
        int temp = this.ptrToNextPage;
        this.ptrToNextPage = splitPage.pageName;
        splitPage.ptrToNextPage = temp;

        //new current page size in bytes
        this.currentSize = calcPageSize(this);
        splitPage.currentSize = calcPageSize(splitPage);

        // add new page to buffer
        StorageManager sm = (StorageManager) StorageManager.getStorageManager();
        if (!sm.getPagebuffer().insertSplitPage(splitPage)) {
            System.err.println("ERORR SPLITTING PAGE, REVERTING PAGE (TODO)");
            return null;
        }
        splitPage.writeToDisk(ACatalog.getCatalog().getDbLocation(), this.IBelongTo);
        this.writeToDisk(ACatalog.getCatalog().getDbLocation(), this.IBelongTo);


        return splitPage;

    }

    public boolean insert(ArrayList<Object> record) {
        this.getPageRecords().add(record);
        this.wasChanged = true;
        this.currentSize += recordSize(record);

        if (currentSize >= Catalog.getCatalog().getPageSize()){
            this.split();
        }
        return true;
    }


    public boolean insert(int idx, ArrayList<Object> record) {
        this.getPageRecords().add(idx, record);
        this.wasChanged = true;
        this.currentSize += recordSize(record);


        if (currentSize >= Catalog.getCatalog().getPageSize()){
            this.split();
        }
        return true;
    }


    public boolean delete(int idx) {
        VerbosePrint.print("REMOVING Record");
        this.currentSize -= recordSize(this.getPageRecords().remove(idx));
        this.wasChanged = true;
        return true;
    }

    public void ClearRecords() {
        this.pageRecords.clear();
        this.wasChanged = true;
    }
}
