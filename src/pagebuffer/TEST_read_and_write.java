package pagebuffer;

import java.nio.ByteBuffer;


import common.Attribute;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class TEST_read_and_write {


    private static String getSaltString(int length) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        return salt.toString();

    }
    private static ArrayList<Object> mkRandomRec() {
        ArrayList<Object> row = new ArrayList<>();
        row.add(69);
        Random r = new Random();
        row.add(r.nextDouble());
        row.add(r.nextBoolean());
        row.add(getSaltString(5));
        row.add(getSaltString(Math.abs(r.nextInt()) % 10 + 1));
        return row;
    }

    public static void main(String[] args) throws IOException {


        // todo TRY TO KEEP TRACK OF HOW MANY bytes WOULD BE half of the records to we ca split pages easy


        String[] schema = "Integer Double Boolean Char(5) varchar(10)".split(" ");
        String fileName = "src/pagebuffer/page1.txt";
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();


        // making N records
        int numrecs = 10;
        for (int i = 0; i < numrecs; i++) {

            //Make random record
            ArrayList<Object> record = mkRandomRec();
            System.out.println("record to store: " + record);

            // make byte array from record
            System.out.println("writing record ");

            // look though reach attribute and check the schema for its type and convert it to its bytes
            // and add it to outputStream btye array
            for (int idx = 0; idx < record.size(); idx++) {
                switch (schema[idx]) {
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
                        if (schema[idx].startsWith("Char")) {
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

        ////////////////////////// reading file given table schema
        System.out.println("reading record from page");

        ////////////// reading in knowing schema
        FileInputStream inputStream = new FileInputStream(fileName);

        // Create data input stream
        DataInputStream dataInputStr = new DataInputStream(inputStream);

        // for each record stored
        System.out.println("\\\\\\\\\\");
        for (int i = 0; i < numrecs; i++) {
            System.out.println("-------------record# " + i + "------------");

            // read in int
            System.out.println(dataInputStr.readInt());

            // read in double
            System.out.println(dataInputStr.readDouble());

            // read in bool ... ect
            System.out.println(dataInputStr.readBoolean());
            System.out.println(new String(dataInputStr.readNBytes(5), StandardCharsets.UTF_8));
            int size = dataInputStr.readInt();
            System.out.println("var char len: " + size);
            System.out.println(new String(dataInputStr.readNBytes(size), StandardCharsets.UTF_8));

        }
    }
}
