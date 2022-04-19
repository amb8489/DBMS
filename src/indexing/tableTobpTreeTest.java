package indexing;

import catalog.ACatalog;
import catalog.Catalog;
import common.*;
import pagebuffer.PageBuffer;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;

import catalog.Catalog;
import common.Attribute;

public class tableTobpTreeTest {

    public static void main(String[] args) {


        Catalog.createCatalog("DB", 1200, 1);
        StorageManager.createStorageManager();

        //////////////////////////////--TABS--/////////////////////////////////////////

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Double"));
        attrs.add(new Attribute("attr2", "Varchar(5)"));

        Catalog.getCatalog().addTable("t1", attrs, attrs.get(0));


        // testing page splitting

        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            System.out.println("INSERT " + b + " " + row);

        }
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));
        StorageManager.pb.PurgeBuffer();

        System.out.println("DONE INSERTING ");

        Table table = (Table) Catalog.getCatalog().getTable("t1");

        int idxI = 1;
        BPlusTree tree = BPlusTree.TreeFromTableAttribute(table,idxI);

        tree.print();

        PageBuffer pb = ((StorageManager)StorageManager.getStorageManager()).getPb();

        for(ArrayList<Object> row :StorageManager.getStorageManager().getRecords(table)){

            ArrayList<RecordPointer> rps = tree.search( ((String)row.get(idxI)) );
            for(RecordPointer rp:rps){


                int pageName =rp.page();
                int idxInPage =rp.index();

                Page page = pb.getPageFromBuffer(String.valueOf(pageName),table);



                System.out.println("should be equal <"+row.get(idxI)+" "+page.getPageRecords().get(idxInPage).get(idxI)+">");

            }






        }







    }
}
