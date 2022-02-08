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



    public static void main(String[] args) throws IOException {


        // todo TRY TO KEEP TRACK OF HOW MANY bytes WOULD BE half of the records to we ca split pages easy



        String[] schema = "Integer Double Boolean Char(5) varchar(10)".split(" ");
        String fileName = "src/pagebuffer/page1.txt";
        DataOutputStream out = new DataOutputStream( new BufferedOutputStream(new FileOutputStream(fileName)));
        ArrayList<ArrayList<Object>> data3 = new ArrayList<>();


        // making records
        int numrecs = 10;
        ArrayList<byte[]> totalRecord= new ArrayList<>();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        for (int i = 0; i < numrecs; i++) {
            ArrayList<Object> row = new ArrayList<>();
            row.add(0);
            Random r = new Random();
            row.add(r.nextDouble());
            row.add(r.nextBoolean());
            row.add(getSaltString(5));
            row.add(getSaltString(Math.abs(r.nextInt()) % 10 + 1));
            data3.add(row);
            System.out.println("record to store: "+ row);

            // writing record to file using schema
            System.out.println("writing record ");

            for (int idx = 0; idx < row.size(); idx++) {
                switch (schema[idx]) {
                    case "Integer":
                        outputStream.write( ByteBuffer.allocate(4).putInt((Integer) row.get(idx)).array() );
                        break;
                    case "Double":
                        outputStream.write(ByteBuffer.allocate(8).putDouble((Double) row.get(idx)).array());
                        break;
                    case "Boolean":
                        outputStream.write(ByteBuffer.allocate(1).put(new byte[]{(byte) ((Boolean) row.get(idx)?1:0)}).array());
                        break;
                    default:
                        // char vs varchar
                        if (schema[idx].startsWith("Char")) {
                            outputStream.write(((String) row.get(idx)).getBytes());
                        } else {
                            int len = ((String) row.get(idx)).length();
                            outputStream.write(ByteBuffer.allocate(4).putInt(len).array());
                            outputStream.write(((String) row.get(idx)).getBytes());
                        }
                }
            }
        }
        // all records
        byte record_out[] = outputStream.toByteArray( );

        // write out records and close file
        out.write(record_out);
        out.close();


        System.out.println("reading record from page");

        ////////////// reading in knowing schema
        FileInputStream inputStream = new FileInputStream(fileName);

        // Create data input stream
        DataInputStream dataInputStr = new DataInputStream(inputStream);

        System.out.println("\\\\\\\\\\");
        for (int i = 0; i < numrecs; i++) {
            System.out.println("-------------record# "+i+"------------");
            System.out.println(dataInputStr.readInt());
            System.out.println(dataInputStr.readDouble());
            System.out.println(dataInputStr.readBoolean());
            System.out.println(new String(dataInputStr.readNBytes(5), StandardCharsets.UTF_8));
            int size = dataInputStr.readInt();
            System.out.println("var char len: " + size);
            System.out.println(new String(dataInputStr.readNBytes(size), StandardCharsets.UTF_8));

        }
    }
}
