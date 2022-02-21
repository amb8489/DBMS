package phase1Testers;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.Table;
import storagemanager.AStorageManager;

import java.util.ArrayList;
import java.util.Random;

public class FullTest {

    // stole this for m the given tester to help make random strings
    private static String getSaltString(int length) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        return salt.toString();

    }
    public static int tot = 342;
    // makes a random record with the schema  " Integer Double Boolean Char(5) varchar(10)"
    public static ArrayList<Object> mkRandomRec(Table table) {
        tot++;


        int charlen = 0;
        int vacharlen = 0;
        ArrayList<String> schema = new ArrayList<>();

        // looping though table attribs to get their types
        for (Attribute att : table.getAttributes()) {
            schema.add(att.getAttributeType());

            // found a char(#) paring for the number
            if (att.getAttributeType().startsWith("Char(")) {
                charlen = Integer.parseInt(att.getAttributeType().substring(5, att.getAttributeType().length() - 1));
            }
            if (att.getAttributeType().startsWith("Varchar(")) {
                vacharlen = Integer.parseInt(att.getAttributeType().substring(8, att.getAttributeType().length() - 1));
            }
        }
        Random r = new Random();
        ArrayList<Object> rec = new ArrayList<>();
        for (int idx = 0; idx < table.getAttributes().size(); idx++) {

                // read in what the schema says is next
                switch (schema.get(idx)) {
                    case "Integer":
                        rec.add(tot);
                        break;
                    case "Double":
                        rec.add(r.nextDouble());
                        break;
                    case "Boolean":
                        rec.add(r.nextBoolean());
                        break;
                    default:
                        if (schema.get(idx).startsWith("Char(")) {
                            rec.add(getSaltString(charlen));
                        } else {
                            rec.add(getSaltString(Math.abs(r.nextInt()) % vacharlen + 1));
                        }
                }
            if((table.pkIdx() != idx) && r.nextInt() % 10 == 0){
                rec.set(idx,null);
            }
        }

        return rec;
    }


    public static void main(String[] args) {
        // mk catalog
        ACatalog cat = Catalog.createCatalog("DB", 4048, 10);

        AStorageManager sm = AStorageManager.createStorageManager();

        // mk table
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Integer"));
        attrs.add(new Attribute("attr2", "Double"));
        attrs.add(new Attribute("attr3", "Boolean"));
        attrs.add(new Attribute("attr4", "Char(5)"));
        attrs.add(new Attribute("attr5", "Varchar(10)"));
        Attribute pk = attrs.get(0);
        Table tab1= null;
        Table tab2= null;
        Table tab3= null;

//
             tab1 = (Table) cat.addTable("table1", attrs, pk);
             tab2 = (Table) cat.addTable("table2", attrs, pk);
             tab3 = (Table) cat.addTable("table3", attrs, pk);

        if (tab1 == null){
                System.out.println("YOU ARE RESTORING TABLE");
                tab1 = (Table) cat.getTable("table1");
                tab2 = (Table) cat.getTable("table2");
                tab3 = (Table) cat.getTable("table3");
                System.out.println(tab1.getPagesThatBelongToMe());
                System.out.println(tab2.getPagesThatBelongToMe());
                System.out.println(tab3.getPagesThatBelongToMe());


        }


//        //see init page made for this table
//
//
//
//        // insert into table
//
        ArrayList<Object> MyRec1 = mkRandomRec(tab1);
        sm.insertRecord(tab1,MyRec1);
//
//
//        System.out.println("------------");
//
//
//
        for (int i = 0; i < 10; i++) {
            sm.insertRecord(tab1,mkRandomRec(tab1));
//            sm.insertRecord(tab2,mkRandomRec(tab2));
//            sm.insertRecord(tab3,mkRandomRec(tab3));

        }
//
//
        ArrayList<Object> MyRec2 = mkRandomRec(tab1);
        MyRec2.set(0,9001);
        sm.insertRecord(tab1,MyRec2);
////
////
////
        sm.purgePageBuffer();
//
        for (ArrayList<Object> r : sm.getRecords(tab1)) {
            System.out.println(r);
        }
//
        System.out.println("GETTING"+MyRec1);
        System.out.println(sm.getRecord(tab1,MyRec1.get(tab1.pkIdx())));
//
        System.out.println("GETTING"+MyRec2);
        System.out.println(sm.getRecord(tab1,MyRec2.get(tab1.pkIdx())));
//
//
//
        System.out.println("REMOVING"+MyRec1);
        System.out.println(sm.deleteRecord(tab1,MyRec1.get(tab1.pkIdx())));
//
//
        System.out.println("REMOVING DNE val"+MyRec1);
        System.out.println(sm.deleteRecord(tab1,null));


        ArrayList<Object> newRecord = mkRandomRec(tab1);
        System.out.println("updating"+MyRec2 +" TO :"+newRecord);
        System.out.println("update:"+sm.updateRecord( tab1, MyRec2, newRecord));
        System.out.println(sm.getRecord(tab1,newRecord.get(tab1.pkIdx())));

//        sm.clearTableData(tab1);
//         save
//        cat.saveToDisk();


        sm.getRecords(tab1).forEach(r ->r.remove(0));
        for (ArrayList<Object> r : sm.getRecords(tab1)) {
            System.out.println(r);
        }
    }
}
