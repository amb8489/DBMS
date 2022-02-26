package phase1Testers;

import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.ITable;
import common.Table;
import parsers.DDLParser;
import parsers.DMLParser;
import storagemanager.AStorageManager;

import java.util.ArrayList;
import java.util.Random;

public class Phase2Testers {

    private static ACatalog cat;
    private static AStorageManager sm;












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
    // makes a random record with the schema
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
                    if (idx == table.pkIdx()){
                        rec.add(tot);
                    }else {
                        rec.add(r.nextInt());

                    }
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
//            if((table.pkIdx() != idx) && r.nextInt() % 10 == 0){
//                rec.set(idx,null);
//            }
        }

        return rec;
    }






    public static void main(String[] args) {

        // mk catalog
        cat = Catalog.createCatalog("DB", 4048, 10);
        sm = AStorageManager.createStorageManager();


        DDLParser.parseDDLStatement("""
                create table class(
                        name Varchar(10) notnull,
                        uid Integer primarykey,
                        primarykey( uid )         
               );"""
        );
        boolean tableMade = cat.containsTable("class");
        System.out.println(cat.getTable("class").tableToString());
        System.out.println("created table class:"+tableMade);


        System.out.println("----------------------------------------------");

        DDLParser.parseDDLStatement("""
                create table student(
                        fname Varchar(10) notnull,
                        lname Varchar(10) notnull,
                        uid Integer primarykey,
                        height Integer,
                        gpa Double,
                        classId Integer,
                        primarykey( uid ),
                        foreignkey( classId ) references class( uid )
                );"""
        );

        tableMade = cat.containsTable("student");
        System.out.println(cat.getTable("student").tableToString());
        System.out.println("created table student:"+ tableMade);

        System.out.println("----------------------------------------------");
        System.out.println("----------------------------------------------");



        int numberOfRows = 10;

        // this is where insert could be tested but for now,

        ITable studentTab = cat.getTable("student");
        for (int i = 0; i < numberOfRows; i++) {
            ArrayList<Object> row = mkRandomRec((Table) studentTab);
            System.out.println(row);
            sm.insertRecord(studentTab,row);
        }

        ///////////////////////////////////////////////////


        DMLParser.parseDMLStatement("delete from student where uid >= 5;");




//        sm.purgePageBuffer();
//        cat.saveToDisk();

    }
}

