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




        //////////////////////////////////////////////////////////////////////////
                                        //v1
        //////////////////////////////////////////////////////////////////////////
        String[] schema = "Integer Double Boolean Char(5) varchar(10)".split(" ");

        // TRY TO KEEP TRACK OF HOW MANY bytes WOULD BE half of the records to we ca split pages easy

        String fileName = "src/pagebuffer/page1.txt";

        // out stream
        DataOutputStream out = new DataOutputStream( new BufferedOutputStream(new FileOutputStream(fileName)));

        // making records
        ArrayList<ArrayList<Object>> data3 = new ArrayList<>();

        int numrecs = 100;
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
                        out.writeInt((Integer) row.get(idx));
                        break;
                    case "Double":
                        out.writeDouble((Double) row.get(idx));
                        break;
                    case "Boolean":
                        out.writeBoolean((Boolean) row.get(idx));
                        break;
                    default:

                        if (schema[idx].startsWith("Char")) {
                            out.writeBytes((String) row.get(idx));
                        } else {
                            int len = ((String) row.get(idx)).length();
//                            System.out.println(len);

                            // storing len of var char
                            out.writeInt(len);
                            // storing varchar
                            out.writeBytes((String) row.get(idx));
                        }
                }
            }
        }
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
