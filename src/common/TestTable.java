package common;

import javax.imageio.IIOException;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class TestTable {








    public static void main(String[] args) {


// output streams
        try {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("src/DB/tabs/tables.txt")));

            // byte array that we will store at the end(all the records stored as bytes at once to reduce the amount of
            // I/O operations)

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


            Table table1 = new Table(name4, attrs4, pk);
            Table table2 = new Table("name5",attrs4,pk);


            Page p = new Page(table1);

            table1.addPageAffiliations(Integer.parseInt(p.getPageName()));
            table1.addPageAffiliations(Integer.parseInt(p.getPageName())+1);
            table1.addPageAffiliations(Integer.parseInt(p.getPageName())+2);
            table1.addPageAffiliations(Integer.parseInt(p.getPageName())+3);
            table1.addPageAffiliations(Integer.parseInt(p.getPageName())+4);


            Page p2 = new Page(table1);

            table2.addPageAffiliations(55);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            outputStream.write(table1.toBytes());
            outputStream.write(table2.toBytes());

            out.write(outputStream.toByteArray());
            out.close();


            Table.ReadAllTablesFromDisk();

            System.exit(1);
        }catch (IOException e){
            System.out.println("ERROR");
            System.exit(1);

        }














        // testing atrributes
        Attribute attrib0 = new Attribute("name", "Varchar");
        Attribute attrib1 = new Attribute("phoneNum", "int");


        ArrayList<Attribute> attrib = new ArrayList<>(Arrays.asList(attrib0, attrib1));

        // test name and id 0
        Table tab0 = new Table("testTab0", attrib,attrib0);
        System.out.println(tab0.getTableName());
        System.out.println(tab0.getTableId());

        // test name change and id 1

        Table tab1 = new Table("testTab1", attrib,attrib0);
        tab1.setTableName("testTab2");
        System.out.println(tab1.getTableName());
        System.out.println(tab1.getTableId());

        System.out.println("\nadding DOB\n");

        // adding atrribute
        tab0.addAttribute("DOB", "char");

        ArrayList<Attribute> att = tab0.getAttributes();
        for (Attribute a : att) {
            System.out.println(a.attributeName());
        }
        System.out.println("\ndrop DOB\n");

        // dropping
        tab0.dropAttribute("DOB");

        att = tab0.getAttributes();
        for (Attribute a : att) {
            System.out.println(a.attributeName());
        }
        System.out.println("\nbad drop / add\n");

        // test drop with nothing
        System.out.println(tab0.dropAttribute("DOB"));

        // test add with already there
        System.out.println(tab0.addAttribute("name","varchar"));

        // getting attribute by name

        System.out.println(tab0.getAttrByName("name").toString());
        // DNE
        System.out.println(tab0.getAttrByName("yerrr"));





    }
}
