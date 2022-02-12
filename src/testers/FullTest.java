package testers;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.ITable;
import common.Page;
import common.Table;
import pagebuffer.PageBuffer;

import java.util.ArrayList;

public class FullTest {


    public static void main(String[] args) {
        // mk catalog
        ACatalog cat = Catalog.createCatalog("src/DB",256,2);

        // make pb this should be made by sm ?
        PageBuffer pb = new PageBuffer(2);




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
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());

        p.writeToDisk("1",tab1);

        p = Page.LoadFromDisk("1",tab1);

        System.out.println("init page size:"+(p.calcSizeOfRecords(p.getPageRecords(),tab1 )+12));

        p = Page.LoadFromDisk("1",tab1);
        p.wasChanged = true;






        // checking on what we now have for pages in table
        System.out.println(tab1.getPagesThatBelongToMe());

        for(int pname:tab1.getPagesThatBelongToMe()){
            p = Page.LoadFromDisk(""+pname,tab1);
            System.out.println(p.getPtrToNextPage());

        }

        p = Page.LoadFromDisk("3",tab1);

        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.getPageRecords().add(TEST_read_and_write.mkRandomRec());
        p.writeToDisk("3",tab1);


        for(int pname:tab1.getPagesThatBelongToMe()){
            p = Page.LoadFromDisk(""+pname,tab1);
            System.out.println(pname +"-->"+p.getPtrToNextPage());

        }






        // save
//        cat.saveToDisk();








    }
}
