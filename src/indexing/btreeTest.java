package indexing;

import common.RecordPointer;

import java.util.Random;

public class btreeTest {


    public static void main(String[] args) {


        var btree = new BPlusTree<Integer>(5);


        System.out.println("-------- inserting ----------");


        Random rand = new Random();
        int N = 100;
        for (int i = 1; i <= N; i++) {
            int int_random = rand.nextInt(20);
            System.out.println(int_random);
            btree.insertRecordPointer(new RecordPointer(0, i), i);
//            btree.insertRecordPointer(new RecordPointer(int_random,int_random),int_random);

        }


        btree.print_h(btree.treeRoot, "");


//        System.out.println("-------- deleting ----------");
//
//        for (int i = 1; i <= N; i++) {
//            int int_random = rand.nextInt(20);
//            System.out.println(int_random);
//
//            btree.removeRecordPointer(new RecordPointer(int_random,int_random),int_random);
//        }
//
//        btree.print(btree.treeRoot, "");


        System.out.println("\n\n-------- searching in----------\n\n");

        int key = 12;
        System.out.println("-------- > "+key+" ----------");

        System.out.println(btree.searchRange(key, false, false));

        System.out.println("-------- >= "+key+" ----------");

        System.out.println(btree.searchRange(key, false, true));

        System.out.println("-------- < "+key+" ----------");

        System.out.println(btree.searchRange(key, true, false));

        System.out.println("-------- <= "+key+" ----------");
        System.out.println(btree.searchRange(key, true, true));

        System.out.println("-------- = "+key+" ----------");

        System.out.println(btree.search(key));


        System.out.println("\n\n-------- searching NOT in----------\n\n");

        key = 15;
        System.out.println("-------- > "+key+" ----------");

        System.out.println(btree.searchRange(key, false, false));

        System.out.println("-------- >= "+key+" ----------");

        System.out.println(btree.searchRange(key, false, true));

        System.out.println("-------- < "+key+" ----------");

        System.out.println(btree.searchRange(key, true, false));

        System.out.println("-------- <= "+key+" ----------");
        System.out.println(btree.searchRange(key, true, true));

        System.out.println("-------- = "+key+" ----------");

        System.out.println(btree.search(key));


        System.out.println("\n\n-------- searching dups----------");

        System.out.println("(adding in numbers 1 - 10 again)\n\n");

        N = 10;
        for (int i = 1; i <= N; i++) {
            btree.insertRecordPointer(new RecordPointer(0, i), i);

        }

        key = 10;
        System.out.println("-------- > "+key+" ----------");

        System.out.println(btree.searchRange(key, false, false));

        System.out.println("-------- >= "+key+" ----------");

        System.out.println(btree.searchRange(key, false, true));

        System.out.println("-------- < "+key+" ----------");

        System.out.println(btree.searchRange(key, true, false));

        System.out.println("-------- <= "+key+" ----------");
        System.out.println(btree.searchRange(key, true, true));

        System.out.println("-------- = "+key+" ----------");

        System.out.println(btree.search(key));





    }
}
