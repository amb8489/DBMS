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

public class testUpdate {

    public static void main(String[] args) {


        Catalog.createCatalog("DB", 500, 1);
        StorageManager.createStorageManager();

        //////////////////////////////--table--/////////////////////////////////////////

        //t1 making new table
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Integer"));
        attrs.add(new Attribute("attr2", "Varchar(5)"));
        Catalog.getCatalog().addTable("t1", attrs, attrs.get(0));


        // adding new empty tree to take

        Table table = (Table) Catalog.getCatalog().getTable("t1");
        table.IndexedAttributes.put(table.getAttributes().get(table.pkIdx()).getAttributeName(), BPlusTree.TreeFromTableAttribute(table, 0));


        System.out.println("----testing update rec----");


        ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
        System.out.print("INSERTING " + row.set(0, 5) + " :");


        boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
        System.out.println(b);

        System.out.println("\n CURRENT TABLE AND TREES");
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));
        table.getPkTree().print();
        table.getPkTree().printRPS();


        ArrayList<Object> newRow = Phase2Testers.mkRandomRec(attrs);
        row.set(0, 5);
        newRow.set(0, 5);


        System.out.println("updating " + row + " --> " + newRow);


        System.out.print("UPDATE STATUS: ");
        b = StorageManager.getStorageManager().updateRecord(Catalog.getCatalog().getTable("t1"), row, newRow);
        System.out.println(b);

        System.out.println("\n NEW TABLE AND TREES");
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));

        table.getPkTree().print();
        table.getPkTree().printRPS();
    }
}
