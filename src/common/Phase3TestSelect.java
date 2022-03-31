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



        //////////////////////////////--cartesian product table--/////////////////////////////////////////

        // cartesian product table atributes

        // 1) adding the attributes from all the tables together
        ArrayList<Attribute> catAt = new ArrayList<>();
        catAt.addAll(attrs);
        catAt.addAll(attrs2);
        catAt.addAll(attrs3);


        // 2) make rows for table
        ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(catAt);
            rows.add(row);
        }

        // makr table
        ResultSet table = Utilities.ResultSetFromTable(catAt, rows);

        /////////////////////////////////////////////////////////////////////////////////



        String statement = """
                           select t1.a, t2.c
                           from t1 t2
                           where t1 > 50
                           orderby t1.a;
                           """;
        Database.executeQuery(statement);
    }
}
