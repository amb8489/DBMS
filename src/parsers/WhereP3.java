package parsers;


import catalog.ACatalog;
import catalog.Catalog;
import common.Attribute;
import common.Table;
import common.Utilities;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;

import java.util.*;

import static java.util.Map.entry;

public class WhereP3 {


    // a cache for already seen statements
    private static HashMap<String, Tuple<List<String>, ArrayList<Tuple<Integer, Integer>>>> CacheWhereStmtPlacementPattern = new HashMap<>();

    // used in the shunting yard algo list of operators and their precedence
    private static final Map<String, Integer> precedence = Map.ofEntries(
            entry("and", 1), entry("or", 2),
            entry("AND", 1), entry("OR", 2),
            entry("=", 3), entry("!=", 3),
            entry("<", 3), entry(">", 3),
            entry("<=", 3), entry(">=", 3));

    // operators used
    private static final Set operators = precedence.keySet();


    // shunting yard algo given a list of tokens
    // evaluates as it parses
    private static boolean Validate(List<String> tokens) {
        try {
            // making stacks for shunting yard algo

            Stack<String> stack = new Stack<>();
            List<Object> Output = new ArrayList<>();

            // look through each tokens
            for (String token : tokens) {

                // is an operator or not
                boolean OpsContains = operators.contains(token);

                // is a (  or  )
                boolean isLeftParentheses = token.equals("(");
                boolean isRightParentheses = token.equals(")");

                // is not any special char
                if (!OpsContains && !isLeftParentheses && !isRightParentheses) {
                    Output.add(token);

                    // if it is a operator
                } else if (OpsContains) {
                    if (!stack.isEmpty()) {

                        // get the top and see what its precedence
                        String top = stack.peek();
                        Integer tokenPrec = precedence.get(token);

                        // pop from stack while precedence of new token <= top of the stack
                        while (!stack.isEmpty() && !top.equals("(") &&
                                operators.contains(top) && tokenPrec <= precedence.get(top)) {


                            // add top to the output
                            String t = stack.pop();
                            Output.add(t);


                            if (Output.size() >= 3 && operators.contains(t)) {
                                Object val = eval(Output.get(Output.size() - 3), Output.get(Output.size() - 2), Output.get(Output.size() - 1));
                                Output = Output.subList(0, Output.size() - 3);
                                Output.add(val);

                            }

                            // repeat
                            if (!stack.isEmpty()) {
                                top = stack.peek();
                            }

                        }
                    }
                    // add the new token
                    stack.push(token);

                } else if (isLeftParentheses) {
                    stack.push(token);

                } else if (isRightParentheses) {

                    while (!stack.isEmpty() && !stack.peek().equals("(")) {
                        String t = stack.pop();
                        Output.add(t);
                        if (Output.size() >= 3 && operators.contains(t)) {
                            Object val = eval(Output.get(Output.size() - 3), Output.get(Output.size() - 2), Output.get(Output.size() - 1));
                            Output = Output.subList(0, Output.size() - 3);
                            Output.add(val);
                        }
                    }

                    if (!stack.isEmpty() && stack.peek().equals("(")) {
                        stack.pop();
                    }
                }
            }

            // at the end remove everything and add to output
            while (!stack.isEmpty()) {
                String t = stack.pop();
                Output.add(t);

                if (Output.size() >= 3 && operators.contains(t)) {
                    Object val = eval(Output.get(Output.size() - 3), Output.get(Output.size() - 2), Output.get(Output.size() - 1));
                    Output = Output.subList(0, Output.size() - 3);
                    Output.add(val);
                }
            }
            // return the result val
            return (boolean) Output.get(Output.size() - 1);


        } catch (Exception exp) {
            System.err.println(exp);
            return false;
        }
    }

    // will evaluate an expression of ether val op val ,  bool op bool
    private static Object eval(Object lexp, Object rexp, Object oplexp) throws Exception {


        String left = lexp.toString();
        String right = rexp.toString();
        String op = oplexp.toString();


        // if op is a and/or then we know bothvals are truth vals (t/f)
        if (((left.equals("true") || left.equals("false")) || (right.equals("true") || right.equals("false")))) {

            if (!((left.equals("true") || left.equals("false")) && (right.equals("true") || right.equals("false")))) {
                System.err.println("COMPARING DIFFERENT TYPES:" + left + " with " + right);
                return false;
            }
        }

        switch (op.toLowerCase()) {

            case "and":
                return left.equals("true") && right.equals("true");
            case "or":
                return left.equals("true") || right.equals("true");
            default:
                break;
        }


        // else vals are variables that need to be checked for truth value
        // ex: 1 < 5


        // check if vals are strings or numeric

        // check if string by looking for a " or trying to parse into a double


        String typeL = Utilities.whatType(left, true);
        String typeR = Utilities.whatType(right, true);


        if (typeL == null || !typeL.equals(typeR)) {
            throw new Exception("COMPARING DIFFERENT TYPES:" + typeL + " with " + typeR);
        }


        // if both vals are strings
        if (typeL.equals("String") || typeL.equals("Boolean")) {

            return switch (op) {
                case "=" -> left.compareTo(right) == 0;
                case "!=" -> left.compareTo(right) != 0;
                case "<" -> left.compareTo(right) < 0;
                case ">" -> left.compareTo(right) > 0;
                case "<=" -> left.compareTo(right) <= 0;
                case ">=" -> left.compareTo(right) >= 0;
                default -> null;
            };


            // both are numeric
        } else {
            Double numLeft = Double.parseDouble(left);
            Double numRight = Double.parseDouble(right);

            return switch (op) {
                case "=" -> numLeft.equals(numRight);
                case "!=" -> !numLeft.equals(numRight);
                case "<" -> numLeft < numRight;
                case ">" -> numLeft > numRight;
                case "<=" -> numLeft <= numRight;
                case ">=" -> numLeft >= numRight;
                default -> null;
            };
        }


        // types don't match
//        throw new Exception("COMPARING DIFFERENT TYPES:" + left + " with " + right);

    }


    // fill string will get a string ready to be run through the validate function
    // string needs to be formated correctly and then be split into tokens
    private static List<String> tokenizer(String whereStmt, String fromStmt, ArrayList<List<Object>> rows) {

        // what tables are referenced in the where
        fromStmt = fromStmt.replace("from","");
        fromStmt = fromStmt.replace(" ","");

        String[] tables = fromStmt.split(",");

        // make sure we have spacing between operators and values
        whereStmt = whereStmt.replace("(", " ( ");
        whereStmt = whereStmt.replace(")", " ) ");

        whereStmt = whereStmt.replace("!", " !");
        whereStmt = whereStmt.replace("<", " < ");
        whereStmt = whereStmt.replace(">", " > ");
        whereStmt = whereStmt.replace("=", " = ");

        whereStmt = whereStmt.replace("<  =", " <= ");
        whereStmt = whereStmt.replace(">  =", " >= ");
        whereStmt = whereStmt.replace("! =", " != ");
        whereStmt = whereStmt.replace(" .", ".");
        whereStmt = whereStmt.replace(". ", ".");

        // tokenize the string by spaces
        List<String> tokens = Utilities.mkTokensFromStr(whereStmt);


        // finding the the WHERE token, we only care what comes after the "where"
        // we start at 1 because we dont want to include the where token just what comes after
        // removing all prefix to where and the where
        int whereIdx = 1;
        for (String t : tokens) {
            if (t.equalsIgnoreCase("where")) {
                tokens = tokens.subList(whereIdx, tokens.size());
                break;
            }
            whereIdx++;
        }

        // gen possible conficts
        Table[] tabsInFrom = new Table[tables.length];
        for ( int i = 0;i< tables.length;i++) {
            String tablename = tables[i];

            Table t = (Table) Catalog.getCatalog().getTable(tablename);
            if (t != null){
                tabsInFrom[i] = t;
            }
        }

        HashSet<String> ConflictCols = AmbiguityCols(tabsInFrom);

        // fill
        for (String token : tokens) {

            // if column name
            if (Utilities.isColName(token)) {

                String[] splitToken = token.split("\\.");

                // if there's a dot then the table name is specified
                if (splitToken.length > 1) {
                    String tableName = splitToken[0];
                    String attributeName = splitToken[1];

                    // check that table exist
                    Table tab = (Table) Catalog.getCatalog().getTable(tableName);
                    if (tab == null) {
                        System.err.println("table " + tableName + " does not exist");
                        return null;
                    }
                    // check that attribute exists in table
                    if (!tab.AttribIdxs.containsKey(attributeName)) {
                        System.err.println("attribute name " + attributeName + " in table " + tableName + " does not exist");
                        return null;
                    }
                    // get idx of that attributeName from that table
                    int idx = tab.AttribIdxs.get(attributeName);

//                    tokens.set(idx, row.get(idx).toString());
//                    System.out.println(tableName + " "+attributeName+" " + idx);
                    continue;
                }




//                 we could have ambiguity with no table name specifer

                    if(ConflictCols.contains(splitToken[0])) {
                        System.err.println("Ambiguous attribute reference: "+ splitToken[0]);
                        return null;
                    }
//              TODO here
//                 check that the attribue belong to only one table (ambiquity check)
//                 make sure that its in at least one table
//
//
//                 find that table and replace that token

//                tokens.set(attrIdx, row.get(attrIdx).toString());

            }
        }

        return null;


    }

    /*--------------    HOW TO USE  -------------------


    -- stmt is a string that follows the where clause.
    -- row is the row in the db being evaluated
    -- attrs is the table.getAttributes()



    EXAMPLE USAGE:

    if you want to delete:
    "delete from foo where (Fname = Aaron and Gpa < 3) or HeightI = 71"

    1) you will pass in "delete from foo where (Fname = Aaron and Gpa < 3) or HeightI = 71"

    2) the row values (aaron,berg,3.4,71,23)

    3) the attribs for that table (Fname,Lname,Gpa,HeightI,Age)

    ...



    if (whereIsTrue("delete from foo where (Fname = Aaron and Gpa < 3) or HeightI = 71", row attribs)){
        delete record
    }
    ...
            */
    public boolean whereIsTrue(String whereStmt, String fromStmt, ArrayList<List<Object>> rows) {

        List<String> tokens = tokenizer(whereStmt, fromStmt, null);

        if (tokens == null) {
            return false;
        }
        return Validate(tokens);
    }


    public static HashSet<String> AmbiguityCols(Table[] tables) {


        //find where attribute names intersect with other tables tables
        HashSet<String> unique = new HashSet<>();
        HashSet<String> notUnique = new HashSet<>();

        for (Table t : tables) {
            for (Attribute aName : t.getAttributes()) {

                // found dup
                if (unique.contains(aName.getAttributeName())) {
                    notUnique.add(aName.getAttributeName());

                    // no dup and yet and place in
                }else {
                    unique.add(aName.getAttributeName());
                }

            }
        }

        return notUnique;

}

    /////////////////////////////////////////// EXAMPLE
    public static void main(String[] args) {


        Catalog.createCatalog("DB", 4048, 3);
        StorageManager.createStorageManager();
        AStorageManager sm = StorageManager.getStorageManager();
        Catalog cat = (Catalog) Catalog.getCatalog();
        WhereP3 parser = new WhereP3();

//        select t1.a, t2.b, t2.c, t3.d
//        from t1, t2, t3
//        where t1.a = t2.b and t2.c = t3.d
//        orderby t1.a


        // TALES

        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("a", "Integer"));
        attrs.add(new Attribute("uidt1", "Integer"));
        cat.addTable("t1", attrs, attrs.get(0));


        //t2
        ArrayList<Attribute> attrs2 = new ArrayList<>();
        attrs2.add(new Attribute("b", "Integer"));
        attrs2.add(new Attribute("c", "Char(3)"));
        attrs2.add(new Attribute("uidt2", "Integer"));
        cat.addTable("t2", attrs2, attrs2.get(0));

        //t3
        ArrayList<Attribute> attrs3 = new ArrayList<>();
        attrs3.add(new Attribute("a", "Integer"));

        cat.addTable("t3", attrs3, attrs3.get(0));

        // adding vales to table t1
        ArrayList<Object> r = new ArrayList<>();
        r.add(1);
        sm.insertRecord(cat.getTable("t1"), r);


        // adding vales to table t2
        ArrayList<Object> r2 = new ArrayList<>();
        r2.add(1);
        sm.insertRecord(cat.getTable("t2"), r2);

        // adding vales to table t3
        ArrayList<Object> r3 = new ArrayList<>();
        r3.add(1);
        sm.insertRecord(cat.getTable("t3"), r3);


        // testing STMT
        long startTime = System.currentTimeMillis();


//        select fName
//        from foo, baz
//        where fName = bName and foo.fName = "aaron";


        String Wherestmt = """
                from t1, t2, t3
                where t1.a = t2.b and t2.c = t3.d or uidt1 = uidt2
                """;


        boolean res = parser.whereIsTrue(
                "where t1.a = t2.b and t2.c = \"str\" and uidt1 = uidt2 and t2.b = z",
                "from t1, t2, t3", null);

        // needs to be table names mapped to its row we are looking at  );
//        System.out.println("STMT: " + res);


        long endTime = System.currentTimeMillis();

        System.out.println(endTime - startTime);

    }

/**
 * author kyle f
 *
 * @param <X> element of type X
 * @param <Y> element of type Y
 */
public static class Tuple<X, Y> {
    public X x;  //element 1
    public Y y;  //element 2

    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return x + " " + y;
    }
}
  /*


    select t1.a, t2.b, t2.c, t3.d
    from t1, t2, t3
    where t1.a = t2.b and t2.c = t3.d
    orderby t1.a;

Strings, unless quoted or true/false, are to be considered column names.


*/
}


