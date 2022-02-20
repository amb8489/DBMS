package phase1Testers;

import catalog.Catalog;
import common.Attribute;
import common.ITable;
import common.Page;
import common.Table;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class TestTable {








    public static void main(String[] args) {


// output streams
        try {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("src/DB/tabs/tables.txt")));

            // byte array that we will store at the end(all the records stored as bytes at once to reduce the amount of
            // I/O operations)
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            ArrayList<Attribute> attrs = new ArrayList<>();
            attrs.add(new Attribute("attr1", "Integer"));
            attrs.add(new Attribute("attr2", "Double"));
            attrs.add(new Attribute("attr3", "Boolean"));
            attrs.add(new Attribute("attr4", "Char(5)"));
            attrs.add(new Attribute("attr5", "Varchar(10)"));
            Random rnd = new Random();


            ArrayList<Table>all_tables = new ArrayList<>();

            for (int i = 0;i < 10 ;i++) {

                // make table
                String name4 = "table "+i;

                // mk random table

                ArrayList<Attribute> Tattrs = new ArrayList<>();
                for (int b = 0;b < rnd.nextInt(1,10) ;b++) {
                    Tattrs.add(attrs.get(rnd.nextInt(4)));
                }
                Attribute pk = Tattrs.get(0);

                Table table1 = new Table(name4, Tattrs, pk);
                Page p = new Page(table1);
                table1.addPageAffiliations(Integer.parseInt(p.getPageName()) + 1);
                table1.addPageAffiliations(Integer.parseInt(p.getPageName()) + 2);
                table1.addPageAffiliations(Integer.parseInt(p.getPageName()) + 3);
                table1.addPageAffiliations(Integer.parseInt(p.getPageName()) + 4);
                all_tables.add(table1);

            }
            for(Table t:all_tables){
                outputStream.write(t.toBytes());
            }

            out.write(outputStream.toByteArray());
            out.close();


            ArrayList<ITable> tbs = Table.ReadAllTablesFromDisk();
            System.out.println(tbs.size());

            for(ITable t: tbs){
                System.out.println(((Table)t).getPagesThatBelongToMe());
            }







            return;
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
