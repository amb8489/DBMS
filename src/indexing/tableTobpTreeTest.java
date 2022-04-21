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


        Catalog.createCatalog("DB", 100000, 100);
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

        //TEST
        //  - key is null error
        //   [933, XCAXY] [374, UWMH7]  [10, OAP] [733, A013] [139, MPFKL] [622, WKI4] [489 aaaaa]
        // also - idx out of bounds eorr
        // [481, 1][114, P][133, QW1P][826, G5][390, MX3][67, 7][577, B5K][580, OUW][332, A8W][649, A]
        for (int i = 0; i < 100000; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(attrs);
//            row.set(0, rand.nextInt(1000));
            row.set(0, i);

            boolean b = StorageManager.getStorageManager().insertRecord(Catalog.getCatalog().getTable("t1"), row);
            System.out.println("INSERT (#"+i+") "+  b + " " + row);

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
