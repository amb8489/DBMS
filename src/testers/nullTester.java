package testers;


import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class nullTester {

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
    public static ArrayList<Object> mkRandomRec() {
        ArrayList<Object> row = new ArrayList<>();
        row.add(69);
        Random r = new Random();
        row.add(r.nextDouble());
        row.add(r.nextBoolean());
        row.add(getSaltString(5));
        row.add(getSaltString(Math.abs(r.nextInt()) % 10 + 1));
        return row;
    }


    public static void main(String[] args) {

        ArrayList<Object> row = mkRandomRec();

        row.set(0, null);
        row.set(2, null);
        //To get List of indexes:

        int[] nullIndexes = IntStream.range(0, row.size()).filter(i -> row.get(i)== null).toArray();
        BitSet bitSet = new BitSet(row.size());
        for(int idx:nullIndexes){bitSet.set(idx);}

        System.out.println("read/write in this many bytes len:"+bitSet.toByteArray().length);


        System.out.println(bitSet.get(0));

//        for(int b:bitSet.toByteArray()){
//           System.out.println(b);
//       }


        System.out.println(row);
    }
}
