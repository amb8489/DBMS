package common;

// a class for helpul utilities

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utilities {

// list of key words not allowed for use for table/ column names

    //TODO add types
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

    public static String whatType(String s,boolean simplifyTypes) {


            if (!simplifyTypes){
                return whatType(s);
            }

            String type =  whatType(s);

            if(type == null){
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
    public static boolean isStringTooLong(String attribute, String string){

        string = string.replace("\"","");



        String type = attribute.toLowerCase();
        if (type.startsWith("char(") || type.startsWith("varchar(")) {


            // lets get how big the string can be and if its too big or small

            int idxOfLeftParen = type.indexOf("(")+1;
            int idxOfRightParen = type.indexOf(")");

            int StringSize = Integer.parseInt(type.substring(idxOfLeftParen, idxOfRightParen));

            // can simpilfy but for ltr
            if (type.toLowerCase().startsWith("char(")) {
                if (string.length() > StringSize) {
                    return true;
                }


            } else if (type.toLowerCase().startsWith("varchar(")) {
                if (string.length() > StringSize) {
                    return true;
                }


            }

            return false;

        }else {
            return true;
        }
    }

    public static Boolean isColName(String s) {

        if (s.equalsIgnoreCase("true") ||
                s.equalsIgnoreCase("false")||
                s.equalsIgnoreCase("and")||
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

        s = s.replace(".","");

        return !isiIllLegalName(s);


    }


    public static HashSet<String> AmbiguityCols(Table table) {


        //find where attribute names intersect with other tables tables
        HashSet<String> unique = new HashSet<>();
        HashSet<String> notUnique = new HashSet<>();

            for (Attribute aName : table.getAttributes()) {
                String Name = aName.getAttributeName();

                String[] SplitAName = aName.getAttributeName().split("\\.");

                if (SplitAName.length > 1){
                    Name = SplitAName[1];
                }

                // found dup
                if (unique.contains(Name)) {
                    notUnique.add(Name);
                    // no dup and yet and place in
                }else {
                    unique.add(Name);
                }

            }

        return notUnique;

    }
}
