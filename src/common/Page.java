package common;

import catalog.Catalog;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Page {

    // number of pages in DB and also used as the name of the page
    private static int numPages = 0;

    // WAS THE RECORDS CHANGED AT ALL FROM INSERT DELETE SPLIT ECT?
    private boolean wasChanged = false;

    //like a linked list
    // ptr to the next page as an int: negitive number means the page points to null
    // name of the page that this page points to would be toString(this.ptrToNextPage)
    private int ptrToNextPage;

    // name of the page aka string of what # page this is
    private String pageName;

    // max size a page can be (still unsure about this)
    private int MaxSize;

    // current size of page in number of records stored (i think this may need to be bytes but easy fix)
    private int currentSize;

    // list of records
    private List<ArrayList<Object>> pageRecords = new ArrayList<>();
    private String IBelongTo = null;

    // makes the first new page
    public Page() {
        numPages++;
        this.currentSize = 0;
        this.pageName = String.valueOf(numPages);
        // first page points to nothing
        this.ptrToNextPage = -1;

    }

    // used when splitting a page
    public Page(List<ArrayList<Object>> records) {

        numPages++;
        this.currentSize = records.size();
        this.pageName = String.valueOf(numPages);
        this.pageRecords =  records;
    }


    public int getPtrToNextPage() {
        return ptrToNextPage;
    }

    public List<ArrayList<Object>>  getPageRecords() {
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
    public boolean LoadFromDisk(String location, ITable table) {

        try {
            // clear out recods just in case
            pageRecords.clear();

            this.IBelongTo = table.getTableName();

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

            System.out.println("reading records from page");


            // read in streams
            FileInputStream inputStream;
            inputStream = new FileInputStream(location);
            DataInputStream dataInputStr = new DataInputStream(inputStream);


            // reading all the records from page from disk

            // first thing thats stored in a page is the num of records stored and its ptr to the next page in linked list
            int numRecs = dataInputStr.readInt();
            this.ptrToNextPage = dataInputStr.readInt();

            //for each row in the page
            for (int rn = 0; rn < numRecs; rn++) {


                ArrayList<Object> rec = new ArrayList<>();

                //for each attribute in the row we use the schema to know how many bytes to read in
                // then store that in rec to make the row
                for (int idx = 0; idx < table.getAttributes().size(); idx++) {

                    // read in what the schema says is next
                    switch (schema.get(idx)) {
                        case "Integer":
                            rec.add(dataInputStr.readInt());
                            break;
                        case "Double":
                            rec.add(dataInputStr.readDouble());
                            break;
                        case "Boolean":
                            rec.add(dataInputStr.readBoolean());
                            break;
                        default:

                            // we get a char(#)
                            if (schema.get(idx).startsWith("Char(")) {

                                rec.add(new String(dataInputStr.readNBytes(charlen), StandardCharsets.UTF_8));
                            } else {
                                // var char should have an int before it telling how long the var char is
                                // and how many bytes to read in
                                int VarCharsize = dataInputStr.readInt();

                                // then read in that many bytes
                                rec.add(new String(dataInputStr.readNBytes(VarCharsize), StandardCharsets.UTF_8));

                            }
                    }
                }

                // append row to pageRecords
                pageRecords.add(rec);
            }
            return true;

            // failure to find page or read fail
        } catch (IOException e) {
            return false;
        }
    }

    // this will write the page to disk at location given the table the the page belongs to
    public boolean writeToDisk(String location, ITable table) {
        try {
            // get schema from table that we need in order to know what type we are reading in
            // if record has a Char(#) type we need to know how long that char is so we know how many bytes to read in
            ArrayList<String> schema = new ArrayList<>();
            for (Attribute att : table.getAttributes()) {
                schema.add(att.getAttributeType());
            }
            // output streams
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(location)));

            // byte array that we will store at the end(all the records stored as bytes at once to reduce the amount of
            // I/O operations)
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();


            // WRITE num records to page and name of next page (nullptr to next = -1)
            outputStream.write(ByteBuffer.allocate(4).putInt(this.pageRecords.size()).array());
            outputStream.write(ByteBuffer.allocate(4).putInt(this.ptrToNextPage).array());


            System.out.println("storing record");

            //for each row in the table
            for (int i = 0; i < this.pageRecords.size(); i++) {

                //for each attrib in row store to byte array
                ArrayList<Object> record = this.pageRecords.get(i);

                // make byte array from record
                // look though reach attribute and check the schema for its type and convert it to its bytes
                // and add it to outputStream btye array
                for (int idx = 0; idx < record.size(); idx++) {
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


            // all records added to byte array
            byte[] record_out = outputStream.toByteArray();

            // write out byte array to file
            out.write(record_out);
            out.close();
            return true;
        } catch (IOException e) {
            System.err.println("COULD NOT FILE PAGE");
            return false;
        }
    }

    // splits this page into two and returns the a new page with the bottom half of the the data
    public Page split(){

        this.wasChanged = true;

        // find half way point
        int half = (int) Math.floor(pageRecords.size()/2.0);

        // split the records in two
        List<ArrayList<Object>> rightHalf = pageRecords.subList(half, pageRecords.size());
        // update this pages records
        this.pageRecords = pageRecords.subList(0, half);

        //Set new page to point to whateber this page points to and then set this to point to new page
        // like adding a node in a linked list

        Page SplitPage = new Page(rightHalf);
        SplitPage.ptrToNextPage = this.ptrToNextPage;
        this.ptrToNextPage = Integer.parseInt(SplitPage.pageName);

        //TODO add page to catalog


        return SplitPage;

    }
}
