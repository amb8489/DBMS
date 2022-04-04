package common;

// a class for helpul utilities

import catalog.Catalog;
import parsers.ResultSet;
import phase2tests.Phase2Testers;
import storagemanager.StorageManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utilities {

// list of key words not allowed for use for table/ column names

    private static Set<String> KEYWORDS = Stream.of(
            "create", "table", "drop", "insert", "into", "delete", "from", "where", "update",
            "notnull", "primarykey", "foreignkey", "references", "add", "default", "set",
            "values", "null", "Integer", "Double", "Boolean", "Varchar", "Char").collect(Collectors.toCollection(HashSet::new));


    // check that a type for an atrribute is legal
    public static boolean isNotLegalType(String TypeName) {
        TypeName = TypeName.toLowerCase();
        switch (TypeName) {
            case "integer":
            case "double":
            case "boolean":
                return false;
            default:
                if (TypeName.startsWith("char(") || TypeName.startsWith("varchar(")) {
                    int Lparen = TypeName.indexOf("(");
                    int Rparen = TypeName.indexOf(")");

                    // ()
                    if (Rparen == Lparen + 1 || Rparen == -1) {
                        return true;
                    }
                    //(nums)
                    String num = TypeName.substring(Lparen + 1, Rparen);
                    // all numbers in str
                    return !num.chars().allMatch(Character::isDigit);
                }
        }
        return true;

    }

    // will decide if a name follows rules of
    // 1) not being a key word && 2) starting with a letter and only having alphanumeric
    public static boolean isiIllLegalName(String name) {

        if (KEYWORDS.contains(name)) {
            System.err.println("name: " + name + " is a keyword and cant be used");
            return true;
        }
        if (!(name.chars().allMatch(Character::isLetterOrDigit) && (name.charAt(0) >= 'A' && name.charAt(0) <= 'z'))) {
//            System.err.println("name must start with letter and only contain alphanumerics");
            return true;
        }

        return false;
    }

    // will format a string removing all redundent whitespace
    public static String format(String stmt) {

        // step 1 remove all new line chars
        stmt = stmt.replace("\n", " ").strip();
        // step 2 remove any redundant spaces

        StringBuilder str = new StringBuilder();
        boolean insideQuoats = false;
        boolean SpaceAlreadySeen = false;

        for (int i = 0; i < stmt.length(); i++) {
            char ch = stmt.charAt(i);

            if (!insideQuoats) {

                if (ch == ' ') {

                    if (!SpaceAlreadySeen) {
                        str.append(ch);
                        SpaceAlreadySeen = true;
                    }

                } else {
                    str.append(ch);
                    if (ch == '\"') {
                        insideQuoats = true;
                    }
                    SpaceAlreadySeen = false;
                }
            } else {
                str.append(ch);
                if (ch == '\"') {
                    insideQuoats = false;
                }
            }

        }
        return str.toString();
    }

    // will make a token list by splitting on spaces after removing all redundant spaces and newlines
    public static List<String> mkTokensFromStr(String stmt) {
        stmt = format(stmt);
        List<String> tokens = new ArrayList<>();

        StringBuilder tk = new StringBuilder();
        boolean inQuoats = false;
        for (int i = 0; i < stmt.length(); i++) {
            char ch = stmt.charAt(i);

            if (!inQuoats && ch == ' ') {

                if (!tk.toString().isBlank()) {
                    tokens.add(tk.toString());
                    tk = new StringBuilder();
                }
            } else {

                if (ch == '\"') {
                    inQuoats = !inQuoats;
                }
                tk.append(ch);
            }
        }
        if (tk.length() > 0) {
            tokens.add(tk.toString());
        }

        tokens.removeIf(String::isBlank);

        return tokens;
    }

    public static String whatType(String s, boolean simplifyTypes) {


        if (!simplifyTypes) {
            return whatType(s);
        }

        String type = whatType(s);

        if (type == null) {
            return null;
        }

        return switch (type.toLowerCase()) {
            case "double", "integer" -> "Numeric";
            default -> type;
        };


    }

    public static String whatType(String s) {

        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return "Boolean";
        }

        // is it a string?
        if (s.contains("\"")) {
            return "String";
        }


        // looking for any char, no base 10 number has a char in it
        for (int i = 0; i < s.length(); i++) {
            if (Character.isLetter(s.charAt(i))) {
                return "String";
            }
        }

        //number can have a . but only one
        boolean periodSeen = false;
        for (int i = 0; i < s.length(); i++) {

            // check for only one .
            if (s.charAt(i) == '.') {
                if (!periodSeen) {
                    periodSeen = true;
                } else {
                    // not a type
                    return null;
                }

            }
        }


        if (periodSeen) {
            return "Double";
        }

        // if just numbers with no . then int
        return "Integer";


    }

    // given a char(#) or varchar(#) attribute and a string itll see if that string is too long/short for the #
    public static boolean isStringTooLong(String attribute, String string) {

        string = string.replace("\"", "");


        String type = attribute.toLowerCase();
        if (type.startsWith("char(") || type.startsWith("varchar(")) {


            // lets get how big the string can be and if its too big or small

            int idxOfLeftParen = type.indexOf("(") + 1;
            int idxOfRightParen = type.indexOf(")");

            int StringSize = Integer.parseInt(type.substring(idxOfLeftParen, idxOfRightParen));

            // can simpilfy but for ltr
            if (type.toLowerCase().startsWith("char(")) {
                return string.length() > StringSize;
            } else if (type.toLowerCase().startsWith("varchar(")) {
                return string.length() > StringSize;
            }

            return false;

        } else {
            return true;
        }
    }

    public static Boolean isColName(String s) {

        if (s.equalsIgnoreCase("true") ||
                s.equalsIgnoreCase("false") ||
                s.equalsIgnoreCase("and") ||
                s.equalsIgnoreCase("or")) {
            return false;
        }
        if (KEYWORDS.contains(s)) {
            return false;
        }

        // is it a string?
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return false;
        }

        s = s.replace(".", "");

        return !isiIllLegalName(s);
    }

    //
    public static ArrayList<ArrayList<Object>> SortBy(Table table, ArrayList<ArrayList<Object>> rows, String AttributeName, Boolean accenting) {
        // table does not have that atrribute
        if (!table.AttribIdxs.containsKey(AttributeName)) {
            System.err.println("Sorting on unknown attribute: " + AttributeName);
            return null;
        }

        // what column you want to sort on is ambiguous
        if (AmbiguityCols(table).contains(AttributeName)) {
            System.err.println("Sorting on ambiguous attribute: " + AttributeName);
            return null;
        }


        String type = table.getAttributes().get(table.AttribIdxs.get(AttributeName)).getAttributeType().toLowerCase();

        int idx = table.AttribIdxs.get(AttributeName);

        if (type.startsWith("varchar") || type.startsWith("char")) {

            //comparitor based on type

            rows.sort(Comparator.comparing(r -> r.get(idx).toString()));
        } else if (type.startsWith("int") || type.startsWith("double")) {
            if (type.startsWith("int")) {
                rows.sort(Comparator.comparing(r -> (Integer) r.get(idx)));
            } else {
                rows.sort(Comparator.comparing(r -> (Double) r.get(idx)));
            }
        } else {
            rows.sort(Comparator.comparing(r -> ((Boolean) r.get(idx))));
        }
        return rows;
    }

    public static void prettyPrintResultSet(ResultSet table, Boolean truncate, int maxSizeTruncate) {


        int spacingSeparation = 0;                   // number of spaces between columns
        int maxStrSize = 200;

        if (truncate) {
            maxStrSize = maxSizeTruncate;
        }

        int[] max = new int[table.attrs().size()];


        for (ArrayList<Object> r : table.results()) {
            for (int i = 0; i < r.size(); i++) {
                Object o = r.get(i);
                max[i] = Math.max(Math.max(max[i], o.toString().length()), table.attrs().get(i).getAttributeName().length());
                if (o.toString().length() > maxStrSize) {
                    r.set(i, o.toString().substring(0, maxStrSize - 4) + "...");
                }
            }
        }

        int[] spacing = new int[table.attrs().size()];
        for (int i = 0; i < table.attrs().size(); i++) {
            spacing[i] = Math.min(max[i], maxStrSize) + spacingSeparation;   // total spac
            spacing[i] += 1;
        }


        String[] atters = new String[table.attrs().size()];

        for (int i = 0; i < table.attrs().size(); i++) {
            Attribute a = table.attrs().get(i);
            atters[i] = a.getAttributeName();
        }


        StringBuilder formatStr = new StringBuilder();
        formatStr.append("║ %-").append(spacing[0]);
        for (int i = 1; i < table.attrs().size(); i++) {

            int idx = i % table.attrs().size();
            formatStr.append("s ║%-").append(spacing[idx]);

        }
        formatStr.append("s║\n");
        String ColNames = String.format(formatStr.toString(), atters);
        System.out.println("═".repeat(ColNames.length() - 1));

        System.out.print(ColNames);
        System.out.println("═".repeat(ColNames.length() - 1));


        for (int i = 0; i < table.results().size(); i++) {
            String rowstr = table.results().get(i).toString().substring(1, table.results().get(i).toString().length() - 1);
            String[] rowStrarr = rowstr.split(",");
            System.out.printf(formatStr.toString(), rowStrarr);
//            System.out.println("".repeat(ColNames.length() - 1));

        }

        System.out.println("═".repeat(ColNames.length() - 1));


    }


    public static ResultSet ResultSetFromTable(ArrayList<Attribute> attributes, ArrayList<ArrayList<Object>> rows) {
        return new ResultSet(attributes, rows);
    }


    public static HashSet<String> AmbiguityCols(Table table) {


        //find where attribute names intersect with other tables tables
        HashSet<String> unique = new HashSet<>();
        HashSet<String> notUnique = new HashSet<>();

        for (Attribute aName : table.getAttributes()) {
            String Name = aName.getAttributeName();

            String[] SplitAName = aName.getAttributeName().split("\\.");

            if (SplitAName.length > 1) {
                Name = SplitAName[1];
            }

            // found dup
            if (unique.contains(Name)) {
                notUnique.add(Name);
                // no dup and yet and place in
            } else {
                unique.add(Name);
            }

        }

        return notUnique;

    }

    public static void main(String[] args) {

        // TALES

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("t1.a", "Integer"));
        attrs.add(new Attribute("t1.uidt1", "Integer"));


        //t2
        ArrayList<Attribute> attrs2 = new ArrayList<>();
        attrs2.add(new Attribute("t2.b", "Char(20)"));
        attrs2.add(new Attribute("t2.c", "Varchar(20)"));
        attrs2.add(new Attribute("t2.uidt2", "Boolean"));

        //t3
        ArrayList<Attribute> attrs3 = new ArrayList<>();
        attrs3.add(new Attribute("t3.a", "Double"));
        attrs3.add(new Attribute("t3.uidt3", "Integer"));


        // cartesian product table

        // 1) adding the attributes from all the tables together
        ArrayList<Attribute> catAt = new ArrayList<>();
        catAt.addAll(attrs);
        catAt.addAll(attrs2);
        catAt.addAll(attrs3);

        // 2 sample row from new table
        ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();

        for (int i = 0; i < 100; i++) {
            ArrayList<Object> row = Phase2Testers.mkRandomRec(catAt);
            rows.add(row);
        }


        Catalog cat = (Catalog) Catalog.createCatalog("DB", 4048, 6);
        StorageManager sm = ((StorageManager) StorageManager.createStorageManager());

        Catalog.getCatalog().addTable("catAt", catAt, catAt.get(0));

        Table t = (Table) cat.getTable("catAt");

        for (ArrayList<Object> ro : rows) {
            sm.insertRecord(t, ro);
        }
        sm.keepWhere(t, "where t1.a > 50", false);

//        if (!Select(t, "t2.b, t2.uidt2,t1.a")) {
//            return;
//        }

        rows = Utilities.SortBy(t, sm.getRecords(t), "t2.uidt2", false);

        ResultSet table = new ResultSet(t.getAttributes(), rows);

        prettyPrintResultSet(table, false, 10);
    }

    public static boolean Select(Table table, HashSet<String> KeepNames){
        ArrayList<Attribute> atters = (ArrayList<Attribute>) table.getAttributes().clone();


        for (Attribute name : atters) {
            if(!KeepNames.contains(name.getAttributeName())){
                StorageManager.getStorageManager().dropAttributeValue(table, table.AttribIdxs.get(name.getAttributeName()));
            }
        }
        return true;
    }
}
