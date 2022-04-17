package indexing;

import common.RecordPointer;

import java.util.Random;

public class btreeTest {


    public static void main(String[] args) {



        var btree = new BPlusTree<Integer>(5);


        System.out.println("-------- inserting ----------");

        Random rand = new Random();
        int N = 20;
        for (int i = 1; i <= N; i++) {
            int int_random = rand.nextInt(20);
            System.out.println(int_random);
            btree.insertRecordPointer(new RecordPointer(int_random,int_random),int_random);
        }


        btree.print(btree.treeRoot,"");


        System.out.println("-------- deleting ----------");

//        for (int i = 1; i <= N; i++) {
//            int int_random = rand.nextInt(100);
//            btree.removeRecordPointer(new RecordPointer(int_random,int_random),int_random);
//        }


        System.out.println("-------- searching ----------");

        int searchKey = 12;
        System.out.println("found for "+searchKey+":  "+btree.search(searchKey));

        btree.print(btree.treeRoot,"");
        System.out.println(   btree.searchRange(12,false,false));




    }
}
