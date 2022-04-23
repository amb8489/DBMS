package indexing.TESTS;

import catalog.Catalog;
import common.Attribute;
import common.Table;
import indexing.BPlusTree;
import pagebuffer.PageBuffer;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.Random;

public class testDelete {

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
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            row.set(0, i);
            System.out.print("INSERTING (#" + i + ") " + " " + row + " :");

            boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
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

            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            row.set(0, i);
//            row.set(0, i);
            System.out.print("(#" + i + ") Deleting: " + row + " :");

            boolean succ = StorageManager.getStorageManager().deleteRecord(Catalog.getCatalog().getTable("t1"), row.get(table.pkIdx()));
            System.out.println(succ);
            System.out.println("----TREE AFTER-----");
            table.getPkTree().print();
            System.out.println("-------------------");

            System.out.println(((StorageManager)StorageManager.getStorageManager()).getRecords(table));


        }













    }

}
