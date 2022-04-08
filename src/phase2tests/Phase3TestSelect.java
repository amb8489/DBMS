package phase2tests;

import catalog.Catalog;
import common.Attribute;
import database.Database;
import parsers.ResultSet;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.Scanner;

public class Phase3TestSelect {


    public static void main(String[] args) {

        // Startup time!  Establish a catalog and a storage manager for us to work with
        Catalog.createCatalog("DB",100 ,2);
        StorageManager.createStorageManager();

        //////////////////////////////--TABS--/////////////////////////////////////////

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("a", "Integer"));

        Catalog.getCatalog().addTable("t1",attrs,attrs.get(0));


        // testing page splitting

        for (int i = 0; i < 22; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            System.out.println(b);
        }
//        StorageManager.getStorageManager().purgePageBuffer();
        System.out.println(StorageManager.getStorageManager().getRecords( Catalog.getCatalog().getTable("t1")));


    }
}
