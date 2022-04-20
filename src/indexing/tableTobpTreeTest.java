package indexing;

import catalog.ACatalog;
import catalog.Catalog;
import common.*;
import pagebuffer.PageBuffer;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.Random;

import catalog.Catalog;
import common.Attribute;

public class tableTobpTreeTest {

    public static void main(String[] args) {


        Catalog.createCatalog("DB", 120, 10);
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
        for (int i = 0; i < 12; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
            row.set(0, rand.nextInt(1000));
            boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            System.out.println("INSERT " + b + " " + row);

        }
        System.out.println(StorageManager.getStorageManager().getRecords(Catalog.getCatalog().getTable("t1")));
        System.out.println("DONE INSERTING ");
        table.getPkTree().print();
        System.exit(1);




        StorageManager.pb.PurgeBuffer();


        int idxI = 1;
        BPlusTree tree = BPlusTree.TreeFromTableAttribute(table, idxI);

        tree.print();

        PageBuffer pb = ((StorageManager) StorageManager.getStorageManager()).getPb();
//
//        for(ArrayList<Object> row :StorageManager.getStorageManager().getRecords(table)){
//
//            ArrayList<RecordPointer> rps = tree.search( ((String)row.get(idxI)) );
//            for(RecordPointer rp:rps){
//
//
//                int pageName =rp.page();
//                int idxInPage =rp.index();
//
//                Page page = pb.getPageFromBuffer(String.valueOf(pageName),table);
//
//
//
//                System.out.println("should be equal <"+row.get(idxI)+" "+page.getPageRecords().get(idxInPage).get(idxI)+">");
//
//            }
//
//
//
//
//
//
//        }
//
//

        tree.writeToDisk();


    }
}
