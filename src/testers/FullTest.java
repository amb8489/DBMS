package testers;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.ITable;
import common.Page;
import common.Table;
import pagebuffer.PageBuffer;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;
import java.util.SortedMap;

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
    public static int tot = 0;
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
        ACatalog cat = Catalog.createCatalog("src/DB", 1024, 2);

        AStorageManager sm = AStorageManager.createStorageManager();

        // mk table
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("attr1", "Integer"));
        attrs.add(new Attribute("attr2", "Double"));
        attrs.add(new Attribute("attr3", "Boolean"));
        attrs.add(new Attribute("attr4", "Char(5)"));
        attrs.add(new Attribute("attr5", "Varchar(10)"));
        Attribute pk = attrs.get(0);
        Table tab1 = (Table) cat.addTable("table1", attrs, pk);


        //see init page made for this table


        System.out.println(tab1.getPagesThatBelongToMe());

        // insert into table
        for (int i = 0; i < 40; i++) {
            ArrayList<Object> rec = mkRandomRec(tab1);
            sm.insertRecord(tab1,rec);
        }

        sm.purgePageBuffer();

        for (ArrayList<Object> r : sm.getRecords(tab1)) {
            System.out.println(r);
        }




        // save
//        cat.saveToDisk();


    }
}
