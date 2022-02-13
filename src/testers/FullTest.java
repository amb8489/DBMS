package testers;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.Page;
import common.Table;
import pagebuffer.PageBuffer;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.SortedMap;

public class FullTest {


    public static void main(String[] args) {
        // mk catalog
        ACatalog cat = Catalog.createCatalog("src/DB",3048,2);

        // make pb this should be made by sm ?
        PageBuffer pb = new PageBuffer(2);

        AStorageManager sm = AStorageManager.createStorageManager();

        // mk table
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Integer"));
        attrs.add(new Attribute("attr2", "Double"));
        attrs.add(new Attribute("attr3", "Boolean"));
        attrs.add(new Attribute("attr4", "Char(5)"));
        attrs.add(new Attribute("attr5", "Varchar(10)"));
        Attribute pk = attrs.get(0);
        Table tab1 = (Table) cat.addTable("table1",attrs,pk);


        //see init page made for this table


        System.out.println(tab1.getPagesThatBelongToMe());

        // insert into table

        //1) if table has no pages we neeed to make its first page :)
        Page p = pb.getPageFromBuffer("1", tab1);

        // add not in any sorted order a new row given the schemea
//        for(int i = 0; i < 30; i++) {
//            p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
//        }
        for(int i = 0; i < 30; i++) {
            ArrayList<Object> row = TEST_read_and_write.mkRandomRec();
            row.set(0, null);
            row.set(4, null);
            p.getPageRecords().add(row);
        }
        p.wasChanged = true;
//
//

//
            for(ArrayList<Object> r: p.getPageRecords()){
                    System.out.println(r);

            }


        pb.PurgeBuffer();

        p = pb.getPageFromBuffer("1",tab1);

        System.out.println(p.calcSizeOfRecords(p.getPageRecords(),tab1));





        // save
//        cat.saveToDisk();








    }
}
