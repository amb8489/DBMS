package common;

import java.util.ArrayList;
import java.util.Arrays;

public class TestTable {








    public static void main(String[] args) {


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
