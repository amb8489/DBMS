package indexing;

import common.RecordPointer;

public class btreeTest {


    public static void main(String[] args) {


        var btree = new BPlusTree<Integer>(5);

        for (int i = 1; i < 10; i++) {
            btree.insertRecordPointer(new RecordPointer(i,i),i);

        }



        btree.insertRecordPointer(new RecordPointer(0,4),8);





    }
}
