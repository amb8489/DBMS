package common;

import catalog.Catalog;
import database.Database;
import parsers.ResultSet;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.Scanner;

public class Phase3TestSelect {


    public static void main(String[] args) {

        // Startup time!  Establish a catalog and a storage manager for us to work with
        Catalog.createCatalog("DB",4048 ,3);
        StorageManager.createStorageManager();




        /////////////////////////////////////////////////////////////////////////////////



        //////////////////////////////--TABS--/////////////////////////////////////////

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("t1.a", "Integer"));
        attrs.add(new Attribute("t1.uidt1", "Integer"));
        //t2
        ArrayList<Attribute> attrs2 = new ArrayList<>();
        attrs2.add(new Attribute("t2.b", "Char(20)"));
        attrs2.add(new Attribute("t2.c", "Varchar(20)"));
        attrs2.add(new Attribute("t2.uidt2", "Integer"));

        //t3
        ArrayList<Attribute> attrs3 = new ArrayList<>();
        attrs3.add(new Attribute("t3.a", "Double"));
        attrs3.add(new Attribute("t3.uidt3", "Integer"));

        Catalog.getCatalog().addTable("t1", attrs, attrs.get(0));
        Catalog.getCatalog().addTable("t2", attrs2, attrs2.get(0));
        Catalog.getCatalog().addTable("t3", attrs3, attrs3.get(0));

        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            ArrayList<Object> row2 = Phase2Testers.mkRandomRec(attrs2);
            ArrayList<Object> row3 = Phase2Testers.mkRandomRec(attrs3);
            //rows.add(row);
            StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t2"), row2);
            StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t3"), row3);
        }

        String statement = """
                           select t1.a, t2.c
                           from t1, t2
                           where t1 > 50
                           orderby t1.a;
                           """;
        Database.executeQuery(statement);
    }
}
