package common;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.ITable;
import storagemanager.AStorageManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;
public class TestPage {


    public static void main(String[] args) {

        // make table
        String name4 = "table4";
        String schema4 = "table4( attr1 Integer, attr2 Double primarykey, attr3 Boolean, attr4 Char(5), attr5 varchar(10) )";

        ArrayList<Attribute> attrs4 = new ArrayList<>();
        attrs4.add(new Attribute("attr1", "Integer"));
        attrs4.add(new Attribute("attr2", "Double"));
        attrs4.add(new Attribute("attr3", "Boolean"));
        attrs4.add(new Attribute("attr4", "Char(5)"));
        attrs4.add(new Attribute("attr5", "Varchar(10)"));
        Attribute pk = attrs4.get(0);

        
        ITable table1 = new Table(name4,attrs4,pk);

        // make page
        Page p = new Page();

        // read data from page
            p.LoadFromDisk("src/pagebuffer/page1.txt",table1);



        // print out page records that were read in
//        for(Object record:p.getPageRecords()){
//            System.out.println(record);
//        }

        p.writeToDisk("src/pagebuffer/page1.txt",table1);

        p.LoadFromDisk("src/pagebuffer/page1.txt",table1);

        // print out page records that we just wrote in
//                for(Object record:p.getPageRecords()){
//                    System.out.println(record);
//                }



        Page splitPage = p.split();


        for(Object record:p.getPageRecords()){
            System.out.println(record);
        }
        System.out.println("----------------------");
        for(Object record:splitPage.getPageRecords()){
            System.out.println(record);
        }



    }
}
