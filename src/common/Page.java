package common;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Page {

    private static int numPages = 0;


    //negitive number means null
    private int ptrToNextPage;


    private String pageName;
    private int MaxSize;
    private int currentSize;

    private String IBelongTo;

    private List<ArrayList<Object>> pageRecords = new ArrayList<>();

    public Page() {
        numPages++;
        this.currentSize = 0;
        this.pageName = String.valueOf(numPages);
    }

    public Page(List<ArrayList<Object>> records) {

        numPages++;
        this.currentSize = records.size();
        this.pageName = String.valueOf(numPages);
        this.pageRecords =  records;


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

    public boolean LoadFromDisk(String location, ITable table) {

        // clear out recods

        try {
            pageRecords.clear();

            // get schema from table
            int charlen = 0;
            ArrayList<String> schema = new ArrayList<>();
            for (Attribute att : table.getAttributes()) {
                schema.add(att.getAttributeType());
                if (att.getAttributeType().startsWith("Char(")) {
                    charlen = Integer.parseInt(att.getAttributeType().substring(5, att.getAttributeType().length() - 1));
                }
            }
            System.out.println("reading records from page");


            // read streams
            FileInputStream inputStream = null;
            inputStream = new FileInputStream(location);

            DataInputStream dataInputStr = new DataInputStream(inputStream);

            // reading all the records in page

            // get num records and next ptr in a page is first thing stored in page
            int numRecs = dataInputStr.readInt();
            this.ptrToNextPage = dataInputStr.readInt();

            //for each record
            for (int rn = 0; rn < numRecs; rn++) {
                //read in each attribute of row and append to rec

                ArrayList<Object> rec = new ArrayList<>();


                for (int idx = 0; idx < table.getAttributes().size(); idx++) {
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

                            if (schema.get(idx).startsWith("Char(")) {

                                rec.add(new String(dataInputStr.readNBytes(charlen), StandardCharsets.UTF_8));

                            } else {
                                // var char should have an int before it telling how long the var char is
                                int size = dataInputStr.readInt();

//                            System.out.println("var char len: " + size);
                                // then read in that many bytes
                                rec.add(new String(dataInputStr.readNBytes(size), StandardCharsets.UTF_8));

                            }

                    }
                }
                // append row to pageRecords
                pageRecords.add(rec);
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    public boolean writeToDisk(String location, ITable table) {
        try {

            // get table schema
            ArrayList<String> schema = new ArrayList<>();
            for (Attribute att : table.getAttributes()) {
                schema.add(att.getAttributeType());
            }
            // outbute stream

            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(location)));

            // byte array
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();



            // WRITE num records to page and name of next page (even if null)
            outputStream.write(ByteBuffer.allocate(4).putInt(this.pageRecords.size()).array());
            outputStream.write(ByteBuffer.allocate(4).putInt(this.ptrToNextPage).array());




            System.out.println("storing record");

            //for each row
            for (int i = 0; i < this.pageRecords.size(); i++) {

                //for each attrib in row wrote to byte array
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
                            if (schema.get(idx).startsWith("Char(")) {
                                outputStream.write(((String) record.get(idx)).getBytes());
                            } else {
                                // add the len of var char before we write var char
                                int len = ((String) record.get(idx)).length();
                                outputStream.write(ByteBuffer.allocate(4).putInt(len).array());
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

    // splits this page into two by returning one page
    public Page split(){

        int half = (int) Math.floor(pageRecords.size()/2.0);

        List<ArrayList<Object>> rightHalf = pageRecords.subList(half, pageRecords.size());
        this.pageRecords = pageRecords.subList(0, half-1);

        //Set new page to point to whateber this page points to and then set this to point to new page
        // like adding a node in a linked list

        Page SplitPage = new Page(rightHalf);
        SplitPage.ptrToNextPage = this.ptrToNextPage;
        this.ptrToNextPage = Integer.parseInt(SplitPage.pageName);
        return SplitPage;

    }
}
