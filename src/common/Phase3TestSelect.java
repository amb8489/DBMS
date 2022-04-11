package common;

import catalog.Catalog;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;
import java.util.ArrayList;
// import Database; commented out because it doesn't work anymore

public class Phase3TestSelect {


    public static void main(String[] args) {

        // Startup time!  Establish a catalog and a storage manager for us to work with
        Catalog.createCatalog("DB",4048 ,3);
        StorageManager.createStorageManager();




        /////////////////////////////////////////////////////////////////////////////////



        //////////////////////////////--TABS--/////////////////////////////////////////

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("a", "Char(2)"));
        attrs.add(new Attribute("uidt1", "Integer"));
        //t2
        ArrayList<Attribute> attrs2 = new ArrayList<>();
        attrs2.add(new Attribute("b", "Char(20)"));
        attrs2.add(new Attribute("c", "Varchar(20)"));
        attrs2.add(new Attribute("uidt2", "Integer"));

        //t3
        ArrayList<Attribute> attrs3 = new ArrayList<>();
        attrs3.add(new Attribute("a", "Double"));
        attrs3.add(new Attribute("uidt3", "Integer"));

        Catalog.getCatalog().addTable("t1", attrs, attrs.get(0));
        Catalog.getCatalog().addTable("t2", attrs2, attrs2.get(0));
        Catalog.getCatalog().addTable("t3", attrs3, attrs3.get(0));

        for (int i = 0; i < 10; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            ArrayList<Object> row2 = Phase2Testers.mkRandomRec(attrs2);
            ArrayList<Object> row3 = Phase2Testers.mkRandomRec(attrs3);
            //rows.add(row);
            StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t2"), row2);
            StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t3"), row3);
        }

        String statement = """
                           select uidt3
                           from t1, t3;
                           """;
        // deleted execution here because of conflicts Database.executeQuery(statement);

    }
}
