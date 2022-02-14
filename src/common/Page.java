/*
 * Authors: Aaron Beghash, Kyle Ferguson
 */

package common;

import catalog.Catalog;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;
import filesystem.FileSystem;

public class Page {

    // number of pages in DB and also used as the name of the page
    public static int numPages = 0;

    // WAS THE RECORDS CHANGED AT ALL FROM INSERT DELETE SPLIT ECT?
    public boolean wasChanged = false;

    //like a linked list
    // ptr to the next page as an int: negitive number means the page points to null
    // name of the page that this page points to would be toString(this.ptrToNextPage)
    private int ptrToNextPage;

    // name of the page aka string of what # page this is
    private String pageName;

    public int currentSize;

    // list of records
    private List<ArrayList<Object>> pageRecords = new ArrayList<>();

    public ITable IBelongTo = null;


    // makes the first new page head of linked list
    public Page(ITable iBelongTo) {
        numPages++;
        this.currentSize = 0;
        this.pageName = String.valueOf(numPages);
        // first page points to nothing
        this.ptrToNextPage = -1;
        this.IBelongTo = iBelongTo;
        ((Table) this.IBelongTo).addPageAffiliations(numPages);

    }

    // used when splitting a page
    public Page(List<ArrayList<Object>> records, ITable iBelongTo, int sizeInBytes, int PtrToNext) {

        numPages++;
        this.wasChanged = true;
        this.pageName = String.valueOf(numPages);

        this.pageRecords = records;
        this.IBelongTo = iBelongTo;
        this.currentSize = sizeInBytes;
        ((Table) this.IBelongTo).addPageAffiliations(numPages);
        this.ptrToNextPage = PtrToNext;

    }

    // USED WHEN LOADING A PAGE IN FROM MEM
    public Page(int pageName, int ptrToNextPage, int currentSize, List<ArrayList<Object>> pageRecords, ITable iBelongTo) {
        if (!((Table) iBelongTo).getPagesThatBelongToMe().contains(pageName)) {
            numPages++;
        }

        this.pageName = String.valueOf(pageName);
        this.pageRecords = pageRecords;
        this.IBelongTo = iBelongTo;
        this.currentSize = currentSize;
        this.ptrToNextPage = ptrToNextPage;
    }

    public Page(ITable table, int ptrToNextPage) {

    }

    public int getPtrToNextPage() {
        return ptrToNextPage;
    }

    public List<ArrayList<Object>> getPageRecords() {
        return pageRecords;
    }

    public String getPageName() {
        return pageName;
    }

    public void setWasChanged(boolean wasChanged) {
        this.wasChanged = wasChanged;
    }

    public boolean isChanged() {
        return wasChanged;
    }

    // loads into a page all the records and page info from disk
    // given the location of the page on disk and the table that the page belongs to;
    public static Page LoadFromDisk(String location, ITable table) {

        try {


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

            VerbosePrint.print("reading records from page " + location);

            // read in streams
            DataInputStream dataInputStr = FileSystem.createPageDataInStream(location);

            int currentSize = 0;
            // reading all the records from page from disk
            int pageName = dataInputStr.readInt();

            currentSize += 4;

            // the num of records stored and its ptr to the next page in linked list
            int numRecs = dataInputStr.readInt();
            //update size 4 bytes
            currentSize += 4;

            int ptrToNextPage = dataInputStr.readInt();
            currentSize += 4;

            List<ArrayList<Object>> pageRecords = new ArrayList<>();

            //for each row in the page
            for (int rn = 0; rn < numRecs; rn++) {


                // read in int for bitarry size (in bytes)
                int bitMaskSize = dataInputStr.readInt();
                currentSize += 4;

                // read in bitmask
                BitSet bitMask = BitSet.valueOf(dataInputStr.readNBytes(bitMaskSize));
                currentSize += bitMaskSize;

                ArrayList<Object> rec = new ArrayList<>();

                //for each attribute in the row we use the schema to know how many bytes to read in
                // then store that in rec to make the row


                for (int idx = 0; idx < table.getAttributes().size(); idx++) {

                    if (!bitMask.get(idx)) {

                        // read in what the schema says is next
                        switch (schema.get(idx)) {
                            case "Integer":
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
            System.err.println("COULD NOT READ IN PAGE " + location);
            return null;
        }


    }

    public void setPageRecords(List<ArrayList<Object>> pageRecords) {
        this.pageRecords = pageRecords;
    }


    public boolean writeToDisk(String location, ITable table) {
        try {
            location = Catalog.getCatalog().getDbLocation() + "/pages/" + this.pageName;

            // get schema from table that we need in order to know what type we are reading in
            // if record has a Char(#) type we need to know how long that char is so we know how many bytes to read in
            ArrayList<String> schema = new ArrayList<>();
            for (Attribute att : table.getAttributes()) {
                schema.add(att.getAttributeType());
            }


            // output streams
            VerbosePrint.print(location);
            DataOutputStream out = FileSystem.createPageDataOutStream(this.pageName);

            // byte array that we will store at the end(all the records stored as bytes at once to reduce the amount of
            // I/O operations)
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();


            ArrayList<Integer> CumSum = calcSizeOfRecordsCumSum(this.pageRecords, this.IBelongTo);

            VerbosePrint.print("here");

            double pSize = CumSum.get(CumSum.size() - 1);

            Double number_of_extra_split_Pages = Math.floor(pSize / Catalog.getCatalog().getPageSize());
            VerbosePrint.print(number_of_extra_split_Pages);
            if (number_of_extra_split_Pages == 0) {

                    VerbosePrint.print("NO SPLIT NEEDED");

                    // WRITE name ,num records to page, and name of next page (nullptr to next = -1)
                    outputStream.write(ByteBuffer.allocate(4).putInt(Integer.parseInt(this.getPageName())).array());
                    outputStream.write(ByteBuffer.allocate(4).putInt(this.pageRecords.size()).array());
                    outputStream.write(ByteBuffer.allocate(4).putInt(this.ptrToNextPage).array());


                    VerbosePrint.print("storing record");

                    //for each row in the table
                    for (int i = 0; i < this.pageRecords.size(); i++) {

                        //for each attrib in row store to byte array
                        ArrayList<Object> record = this.pageRecords.get(i);


                        //FIRST LETS TAKE CARE OF NULL VALS :)

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


                        // make byte array from record
                        // look though reach attribute and check the schema for its type and convert it to its bytes
                        // and add it to outputStream btye array
                        for (int idx = 0; idx < record.size(); idx++) {
                            if (record.get(idx) != null) {
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

                                        // char vs varchar

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

                    // clear records
//                pageRecords.clear();
                    // update page size
                    currentSize = 0;
                    VerbosePrint.print("Store complete");
                    return true;

            } else {

                //time to split up
                int start = 0;
                int adj = 0;
                int SplitPoint = 0;

                Page WillSplit = this;

                List<ArrayList<Object>> RECORDS = this.pageRecords.subList(0, pageRecords.size());

                CumSum.remove(0);
                VerbosePrint.print(CumSum);
                for (int i = 0; i < CumSum.size(); i++) {
                    if (CumSum.get(i) - adj >= Catalog.getCatalog().getPageSize()) {
                        int numberOfRecordsForpage = (i - start);
                        SplitPoint = numberOfRecordsForpage / 2;
                        adj += CumSum.get(SplitPoint);

                        List<ArrayList<Object>> left = RECORDS.subList(start, start + SplitPoint + 1);
                        VerbosePrint.print("SPLITTING PAGE");

                        start = start + SplitPoint + 1;
                        VerbosePrint.print(RECORDS.size());


                        // split making new page
                        WillSplit.pageRecords = left;
                        VerbosePrint.print(RECORDS.size());


                        // new page will need attribs

                        Page nextPage = new Page(table);
                        nextPage.ptrToNextPage = WillSplit.ptrToNextPage;

                        WillSplit.ptrToNextPage = Integer.parseInt(nextPage.pageName);
                        //write out

                        WillSplit.writeToDisk(WillSplit.getPageName(), table);

                        WillSplit = nextPage;


                    }
                }


                List<ArrayList<Object>> left = RECORDS.subList(start, RECORDS.size());
                // split making new page
                WillSplit.pageRecords = left;

                WillSplit.writeToDisk(WillSplit.getPageName(), table);

                return true;

            }
        } catch (IOException e) {
            System.err.println("COULD NOT Write FILE PAGE " + location);
            return false;
        }
    }


    public ArrayList<Integer> calcSizeOfRecordsCumSum(List<ArrayList<Object>> recs, ITable table) {
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
        ArrayList<Integer> CumSum = new ArrayList<>();
        CumSum.add(12);


        for (int i = 0; i < recs.size(); i++) {

            //for each attrib in row store to byte array
            ArrayList<Object> r = recs.get(i);

            // make byte array from record
            // look though reach attribute and check the schema for its type and convert it to its bytes
            // and add it to outputStream btye array
            int newSize = 0;
            newSize += 4;

            if (r.contains(null)) {
                newSize += 2;
            }
            for (int idx = 0; idx < r.size(); idx++) {

                if (r.get(idx) != null) {
                    switch (schema.get(idx)) {
                        case "Integer":
                            newSize += 4;
                            break;
                        case "Double":
                            newSize += 8;
                            break;
                        case "Boolean":
                            newSize += 1;
                            break;
                        default:

                            // char(#)
                            if (schema.get(idx).startsWith("Char(")) {
                                newSize += charlen;
                            } else {
                                // add the len of var char before we write var char
                                int VarCharlen = ((String) r.get(idx)).length();
                                newSize += (4 + VarCharlen);
                            }
                    }
                }
            }
            CumSum.add(newSize + CumSum.get(CumSum.size() - 1));
        }
        return CumSum;

    }


    public int calcSizeOfRecords(List<ArrayList<Object>> recs, ITable table) {
        ArrayList<Integer> sizeBytes = calcSizeOfRecordsCumSum(recs, table);
        return sizeBytes.get(sizeBytes.size() - 1);

    }
}
