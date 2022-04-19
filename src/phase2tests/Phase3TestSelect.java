package phase2tests;

import catalog.Catalog;
import common.Attribute;
import common.Table;

import parsers.ResultSet;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.Scanner;

public class Phase3TestSelect {


    public static void main(String[] args) {

        // Startup time!  Establish a catalog and a storage manager for us to work with

        Catalog.createCatalog("DB", 4048, 1);
        StorageManager.createStorageManager();

        //////////////////////////////--TABS--/////////////////////////////////////////

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Double"));

        Catalog.getCatalog().addTable("t1", attrs, attrs.get(0));


        // testing page splitting

        for (int i = 0; i < 10; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            System.out.println("INSERT " + b + " " + row);

        }
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));
        StorageManager.pb.PurgeBuffer();

        System.out.println("DONE INSERTING ");


        if (Catalog.getCatalog().getTable("t1").addAttribute("attr2", "Double")) {
            // add default val
            StorageManager.getStorageManager().addAttributeValue(Catalog.getCatalog().getTable("t1"), 2.0);
        }


        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));

        StorageManager.getStorageManager().dropAttributeValue(Catalog.getCatalog().getTable("t1"), 1);

        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));

    }
}
