package common;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Page {


    private String pageName;
    private String IBelongTo;
    private ArrayList<ArrayList<Object>> pageRecords = new ArrayList<>();

    public ArrayList<ArrayList<Object>> getPageRecords() {
        return pageRecords;
    }

    public String getPageName() {
        return pageName;
    }

    public boolean readFromDisk(String location, ITable table) throws IOException {

        // clear out recods
        pageRecords.clear();

        // get schema from table
        ArrayList<String> schema = new ArrayList<>();
        for (Attribute att : table.getAttributes()) {
            schema.add(att.getAttributeType());
        }
        System.out.println("reading records from page");

        // read streams
        FileInputStream inputStream = new FileInputStream(location);
        DataInputStream dataInputStr = new DataInputStream(inputStream);

        // reading all the records in page

        //TODO get num records in a page
        int numRecs = 5;

        //for each record
        for (int rn = 0; rn < numRecs; rn++) {
            //read in each attribute of row and append to rec
            System.out.println("-----------------------------------------");

            // TODO add each attribute to rec and append rec in pageRecords not just print out val
            ArrayList<Object> rec = new ArrayList<>();

            for (int idx = 0; idx < table.getAttributes().size(); idx++) {
                switch (schema.get(idx)) {
                    case "Integer":
                        System.out.println(dataInputStr.readInt());
                        break;
                    case "Double":
                        System.out.println(dataInputStr.readDouble());
                        break;
                    case "Boolean":
                        System.out.println(dataInputStr.readBoolean());
                        break;
                    default:

                        if (schema.get(idx).startsWith("Char")) {

                            // TODO get char len from schema
                            int charLen = 5;
                            System.out.println(new String(dataInputStr.readNBytes(charLen), StandardCharsets.UTF_8));

                        } else {
                            int size = dataInputStr.readInt();
                            System.out.println("var char len: " + size);
                            System.out.println(new String(dataInputStr.readNBytes(size), StandardCharsets.UTF_8));
                        }

                }
            }
            // append row to pageRecords
            pageRecords.add(rec);
        }
        return true;
    }


    public boolean writeToDisk() {
        System.err.println("ERROR: write to disk failed");
        return false;
    }
}
