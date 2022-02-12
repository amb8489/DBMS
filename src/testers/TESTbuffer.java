package testers;

import common.Attribute;
import common.ITable;
import common.Page;
import common.Table;
import pagebuffer.PageBuffer;

import java.io.IOException;
import java.util.ArrayList;

public class TESTbuffer {


    public static void main(String[] args) throws IOException {

        PageBuffer pb = new PageBuffer(2);

        TEST_read_and_write.test();



        ///////// table attributes
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Integer"));
        attrs.add(new Attribute("attr2", "Double"));
        attrs.add(new Attribute("attr3", "Boolean"));
        attrs.add(new Attribute("attr4", "Char(5)"));
        attrs.add(new Attribute("attr5", "Varchar(10)"));

        Table t1 = new Table("t1",attrs,attrs.get(0));

        // testing for loading new page
        Page newpage = pb.getPageFromBuffer("src/DB/pages/1",t1);
        System.out.println(newpage.getPageName());

        System.out.println(pb.getPageBufferSize());

        // testing for loading from cache
        Page samepage = pb.getPageFromBuffer("src/DB/pages/1",t1);
        System.out.println(pb.getPageBufferSize());

        //filling buffer
        Page page2 = pb.getPageFromBuffer("src/DB/pages/2",t1);
        System.out.println(pb.getPageBufferSize());
        Page page2same = pb.getPageFromBuffer("src/DB/pages/2",t1);
        System.out.println(pb.getPageBufferSize());


        //testing LRU wrting should be page 1

        Page page3 = pb.getPageFromBuffer("src/DB/pages/3",t1);
        System.out.println("page 3 name:"+page3.getPageName());
        System.out.println(pb.getPageBufferSize());

        //testing LRU wrting should be page 2

        Page page4 = pb.getPageFromBuffer("src/DB/pages/4",t1);
        System.out.println(pb.getPageBufferSize());
        System.out.println("page 4 name:"+page4.getPageName());


        // testing ordering in LRU array
        System.out.println("-----------");

        page3 = pb.getPageFromBuffer("src/DB/pages/3",t1);
        System.out.println(pb.getPageBufferSize());
        System.out.println("page3 name:"+page3.getPageName());


        // !!!!   EXAMPLE change in page doing an update to page before it gets written out

        // !!!! when we load a new page page 4 should be written out
        page4.getPageRecords().clear();
        page4.wasChanged = true;

        // purging only page 4 write
        pb.PurgeBuffer();
        System.out.println(pb.getPageBufferSize());


        // !!!! loading pagee 4 in to see if changes got saved to disk

         page4 = pb.getPageFromBuffer("src/DB/pages/4",t1);


        System.out.println("page4 name:"+page4.getPageName());

        System.out.println(":"+page4.currentSize);


























    }
}
