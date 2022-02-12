package testers;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.Page;
import common.Table;

import java.io.*;
import java.util.ArrayList;

public class TESTcatalog {

    public static void main(String[] args) throws IOException {





        // ignore part for testing below all this vv
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("src/DB/tabs/tables.txt")));
        String name4 = "table4";
        String schema4 = "table4( attr1 Integer, attr2 Double primarykey, attr3 Boolean, attr4 Char(5), attr5 varchar(10) )";
        ArrayList<Attribute> attrs4 = new ArrayList<>();
        attrs4.add(new Attribute("attr1", "Integer"));
        attrs4.add(new Attribute("attr2", "Double"));
        attrs4.add(new Attribute("attr3", "Boolean"));
        attrs4.add(new Attribute("attr4", "Char(5)"));
        attrs4.add(new Attribute("attr5", "Varchar(10)"));
        Attribute pk = attrs4.get(0);
        Table table1 = new Table(name4, attrs4, pk);
        Table table2 = new Table("name5",attrs4,pk);
        Page p = new Page(table1);
        table1.addPageAffiliations(Integer.parseInt(p.getPageName()));
        table1.addPageAffiliations(2);
        table1.addPageAffiliations(3);
        table1.addPageAffiliations(4);
        table1.addPageAffiliations(Integer.parseInt(p.getPageName())+4);
        Page p2 = new Page(table1);
        table2.addPageAffiliations(55);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(table1.toBytes());
        outputStream.write(table2.toBytes());
        out.write(outputStream.toByteArray());
        out.close();


        // look at this part for testing

        // have to delete  src/DB/catalog/catalog.txt  test restart and all that

        ACatalog c = Catalog.createCatalog("src/DB/catalog/catalog.txt",10,256);

        System.out.println(c.containsTable(table1.getTableName()));
        System.out.println(c.containsTable(table2.getTableName()));
        

        System.out.println(c.getPageSize());
        System.out.println(c.getPageBufferSize());



//        System.out.println(c.getTable(table1.getTableName()).tableToString());
        c.saveToDisk();



    }
}
