package indexing.TESTS;

import catalog.Catalog;
import common.Attribute;
import common.Page;
import common.RecordPointer;
import common.Table;
import indexing.BPlusTree;
import pagebuffer.PageBuffer;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.Random;

public class testInsert {


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


        // testing table inseting
        Random rand = new Random();
        int bound = 1000;

        for (int i = 0; i < 100; i++) {
            var row = Phase2Testers.mkRandomRec(attrs);
            row.set(0, rand.nextInt(bound));
//            row.set(0, i);
            System.out.print("INSERTING (#" + i + ") " + " " + row + " :");

            boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            System.out.println(b);

        }
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));
        System.out.println("DONE INSERTING CURRENT TREES");
        // start node and end leaf node in hashmap from int to start,end nodes
        table.getPkTree().print();
        table.getPkTree().printRPS();


        StorageManager.pb.PurgeBuffer();


        BPlusTree tree = BPlusTree.TreeFromTableAttribute(table, table.pkIdx());

        tree.print();

        PageBuffer pb = ((StorageManager) StorageManager.getStorageManager()).getPb();

        for (var row2 : StorageManager.getStorageManager().getRecords(table)) {

            ArrayList<RecordPointer> rps = tree.search(((Integer) row2.get(table.pkIdx())));
            for (RecordPointer rp : rps) {


                int pageName = rp.page();
                int idxInPage = rp.index();

                Page page = pb.getPageFromBuffer(String.valueOf(pageName), table);


                System.out.println("should be equal <" + row2.get(table.pkIdx()) + " == " + page.getPageRecords().get(idxInPage).get(table.pkIdx()) + ">");

            }


        }


//        tree.writeToDisk();


    }

}
