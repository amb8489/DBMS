package indexing.TESTS;

import catalog.ACatalog;
import catalog.Catalog;
import common.*;
import indexing.BPlusTree;
import pagebuffer.PageBuffer;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;

import catalog.Catalog;
import common.Attribute;

public class tableTobpTreeTest {

    public static void main(String[] args) {


        Catalog.createCatalog("DB", 500, 1);
        StorageManager.createStorageManager();

        //////////////////////////////--TABS--/////////////////////////////////////////

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Integer"));
        attrs.add(new Attribute("attr2", "Varchar(5)"));
        Catalog.getCatalog().addTable("t1", attrs, attrs.get(0));


        Table table = (Table) Catalog.getCatalog().getTable("t1");
        table.IndexedAttributes.put(table.getAttributes().get(table.pkIdx()).getAttributeName(), BPlusTree.TreeFromTableAttribute(table, 0));



        System.out.print("testing update rec");


        ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
        row.set(0, 5);
        System.out.print("INSERTING " + row);
        boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
        System.out.println(b);


        // dup pk test
        System.out.print("INSERTING " + row);
         b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
        System.out.println(b);




        ArrayList<Object> newRow = Phase2Testers.mkRandomRec(attrs);
        row.set(0, 5);
        newRow.set(0, 5);

        System.out.print("updating " + row + " --> "+ newRow);

        System.out.print("DID UPDATE: ");
        b = StorageManager.getStorageManager().updateRecord(Catalog.getCatalog().getTable("t1"), row,newRow);

        System.out.println(b);


        System.out.println("CHANGED TABLE");
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));

        table.getPkTree().print();
        table.getPkTree().printRPS();



        System.exit(1);



        // testing table inseting
        Random rand = new Random();
        int bound = 1;

        for (int i = 0; i < 100; i++) {
             row = Phase2Testers.mkRandomRec(attrs);
            row.set(0, rand.nextInt(bound));
//            row.set(0, i);
            System.out.print("INSERTING (#" + i + ") " + " " + row + " :");

             b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            System.out.println(b);

        }
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));
        System.out.println("DONE INSERTING ");
        // start node and end leaf node in hashmap from int to start,end nodes
        table.getPkTree().print();
        table.getPkTree().printRPS();

        System.out.println("-----removing----- ");


//        for (int i = 50; i > -1; i--) {
        for (int i = 100; i > 51; i--) {

             row = Phase2Testers.mkRandomRec(attrs);
            row.set(0, rand.nextInt(bound));
//            row.set(0, i);
            System.out.print("(#" + i + ") Deleting: " + row + " :");

            boolean succ = StorageManager.getStorageManager().deleteRecord(Catalog.getCatalog().getTable("t1"), row.get(table.pkIdx()));
            System.out.println(succ);
            System.out.println("----TREE AFTER-----");
            table.getPkTree().print();
            System.out.println("-------------------");

            System.out.println(((StorageManager)StorageManager.getStorageManager()).getRecords(table));


        }












        StorageManager.pb.PurgeBuffer();

        int idxI = 1;
        BPlusTree tree = BPlusTree.TreeFromTableAttribute(table, idxI);

        tree.print();

        PageBuffer pb = ((StorageManager) StorageManager.getStorageManager()).getPb();
//
//        for(ArrayList<Object> row :StorageManager.getStorageManager().getRecords(table)){
//
//            ArrayList<RecordPointer> rps = tree.search( ((String)row.get(idxI)) );
//            for(RecordPointer rp:rps){
//
//
//                int pageName =rp.page();
//                int idxInPage =rp.index();
//
//                Page page = pb.getPageFromBuffer(String.valueOf(pageName),table);
//
//
//
//                System.out.println("should be equal <"+row.get(idxI)+" "+page.getPageRecords().get(idxInPage).get(idxI)+">");
//
//            }
//
//
//
//
//
//
//        }
//
//

        tree.writeToDisk();


    }
}
