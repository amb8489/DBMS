package parsers;


import catalog.ACatalog;
import catalog.Catalog;
import common.*;
import indexing.BPlusTree;
import phase2tests.Phase2Testers;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;

import java.util.*;

import static common.Utilities.prettyPrintTable;
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
            throw new Exception("COMPARING DIFFERENT TYPES:" + left + " with " + right);
        }


        // if both vals are strings
        if (typeL.equals("string") || typeL.equals("boolean")) {

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
    private static List<String> tokenizer(String whereStmt, Table table, ArrayList<Object> row) {


//        System.out.println(table.getAttributes());


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

//        System.out.println(whereStmt);
//        System.exit(1);


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
        HashSet<String> ConflictCols = Utilities.AmbiguityCols(table);

        // replace token col names with row val for that attribute
        int tokenIdx = -1;


        for (String token : tokens) {
            tokenIdx++;


            // if column name
            if (Utilities.isColName(token)) {

                String[] splitToken = token.split("\\.");
                // if there's a dot then the table name is specified
//                System.out.println(token);
//                System.exit(1);
                if (splitToken.length > 1) {
                    String tableName = splitToken[0];
                    String attributeName = splitToken[1];


                    // check that attribute exists in table
                    if (!table.AttribIdxs.containsKey(token)) {
                        System.err.println("attribute name " + attributeName + " in table " + tableName + " does not exist");
                        return null;
                    }

                    // get idx of that attributeName from that table
                    int AttributeIdx = table.AttribIdxs.get(token);
                    tokens.set(tokenIdx, row.get(AttributeIdx).toString());
                    continue;
                }

                // if not tableName. specifier


//               we could have ambiguity with no table name specifer
                if (ConflictCols.contains(splitToken[0])) {
                    System.err.println("Ambiguous attribute reference: " + splitToken[0]);
                    return null;
                }

                // make sure that the attribute is in at least one table that was in the from stmt
                // find that table


                if (!table.AttribIdxs.containsKey(splitToken[0])) {
                    System.err.println("attribute does not exist in any of the tables referenced: " + splitToken[0]);
                    return null;
                }
                // get the index of that attrbute
                int AttributeIdx = table.AttribIdxs.get(token);
                tokens.set(tokenIdx, row.get(AttributeIdx).toString());
            }
        }
        VerbosePrint.print(tokens);

        // return the tokens now filled with proer values
        System.out.println(tokens);
//        System.exit(1);
        return tokens;


    }

    public static boolean isAllSimpleCase1(List<String> tokens, Table table) {


        HashSet<String> operators = new HashSet<>(List.of(new String[]{"=", ">", ">=", "<", "<=", "!="}));


        boolean atLeastOneIsIndexed = false;


        for (int i = 0; i < tokens.size(); i += 4) {


            // check that first token is a column name and the third is a value
            var firstIsColumnName = table.AttribIdxs.containsKey(tokens.get(i));
            var thirdIsColumnName = table.AttribIdxs.containsKey(tokens.get(i + 2));


            // if both true or both false then bad
            if (firstIsColumnName == thirdIsColumnName) {
                return false;
            }

            // every 3rd token should be an operator
            var operator = operators.contains(tokens.get(i + 1));

            if (!operator) {
                System.err.println(tokens.get(i + 1));
                return false;
            }

            // check if one of the atributes is indexed

            if (firstIsColumnName && !atLeastOneIsIndexed) {
                atLeastOneIsIndexed = table.IndexedAttributes.containsKey(tokens.get(i));
            } else if (thirdIsColumnName && !atLeastOneIsIndexed) {
                atLeastOneIsIndexed = table.IndexedAttributes.containsKey(tokens.get(i + 2));
            }

        }


        // every 4th token should be "AND"
        for (int i = 0; i < tokens.size(); i += 4) {
            if (i + 3 < tokens.size()) {

                if (!tokens.get(i + 3).equalsIgnoreCase("and")) {
                    return false;
                }
            }
        }
        System.err.println("isAllSimpleCase1: " + atLeastOneIsIndexed);

        return atLeastOneIsIndexed;

    }

    public static boolean isLegalCase2(List<String> tokens, Table table) {



        // for ands we and to get the expression to the left and right


        // find all the index of the and's and the or's

        ArrayList<Integer> AndIdxs = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("AND")) {
                AndIdxs.add(i);
            }
        }


        // ex: fish != "dog" AND snake != poodle
        // we want to make sure (fish != "dog") , (snake != poodle) both expressions have atleast 1 indx
        ArrayList<String[]> expressions = new ArrayList<>();

        for (int AndIdx : AndIdxs) {
            // get the left three tokens and the right three tokens from the and idx


            var leftSideLeft = tokens.get(AndIdx - 3);
            var leftSideOperator = tokens.get(AndIdx - 2);
            var leftSideRight = tokens.get(AndIdx - 1);


            var rightSideLeft = tokens.get(AndIdx + 1);
            var rightSideOperator = tokens.get(AndIdx + 2);
            var rightSideRight = tokens.get(AndIdx + 3);

            var LeftExpression = new String[]{leftSideLeft, leftSideOperator, leftSideRight};
            var RightExpression = new String[]{rightSideLeft, rightSideOperator, rightSideRight};


            // check that first token is a column name and the third is a value
            var firstIsColumnName = table.AttribIdxs.containsKey(LeftExpression[0]);
            var thirdIsColumnName = table.AttribIdxs.containsKey(LeftExpression[2]);


            // if both true or both false then bad
            if (firstIsColumnName == thirdIsColumnName) {
                return false;
            }

            // check that first token is a column name and the third is a value
             firstIsColumnName = table.AttribIdxs.containsKey(RightExpression[0]);
             thirdIsColumnName = table.AttribIdxs.containsKey(RightExpression[2]);


            // if both true or both false then bad
            if (firstIsColumnName == thirdIsColumnName) {
                System.err.println(firstIsColumnName);
                return false;
            }



            // test that at least one is indexed

            boolean atLeastOneIndexed = isLegalIndexExpr(LeftExpression, table) || isLegalIndexExpr(RightExpression, table);


            // failure to have and exploit indexing

            if (!atLeastOneIndexed) {
                return false;
            }


        }



        // we are looking for adjacent ors like   or expr or

        ArrayList<Object[]> logicalOpers = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("OR") || tokens.get(i).equalsIgnoreCase("AND")) {
                logicalOpers.add(new Object[]{tokens.get(i),i});
            }
        }


        // find ajacent ors

        // if or is first and last also

        if (!logicalOpers.isEmpty()) {

            // check to see if the  first operator is an OR

            // expr or ...
            if (((String) logicalOpers.get(0)[0]).equalsIgnoreCase("OR")){
                var middleLeft = tokens.get(0);
                var middleOperator = tokens.get(1);
                var middleRight = tokens.get(2);

                var LeftExpression = new String[]{middleLeft, middleOperator, middleRight};

                // check that first token is a column name and the third is a value
                var firstIsColumnName = table.AttribIdxs.containsKey(LeftExpression[0]);
                var thirdIsColumnName = table.AttribIdxs.containsKey(LeftExpression[2]);


                // if both true or both false then bad
                if (firstIsColumnName == thirdIsColumnName) {
                    return false;
                }

                // test that at least one is indexed

                boolean atLeastOneIndexed = isLegalIndexExpr(LeftExpression, table);


                // failure to have and exploit indexing

                if (!atLeastOneIndexed) {
                    return false;
                }


            }

            // ... or expr
            if (((String) logicalOpers.get(logicalOpers.size()-1)[0]).equalsIgnoreCase("OR")){


                var middleLeft = tokens.get(tokens.size() - 3);
                var middleOperator = tokens.get(tokens.size() - 2);
                var middleRight = tokens.get(tokens.size() - 1);
                var LeftExpression = new String[]{middleLeft, middleOperator, middleRight};

                // check that first token is a column name and the third is a value
                var firstIsColumnName = table.AttribIdxs.containsKey(LeftExpression[0]);
                var thirdIsColumnName = table.AttribIdxs.containsKey(LeftExpression[2]);


                // if both true or both false then bad
                if (firstIsColumnName == thirdIsColumnName) {

                    return false;
                }

                // test that at least one is indexed

                boolean atLeastOneIndexed = isLegalIndexExpr(LeftExpression, table);

                // failure to have and exploit indexing

                if (!atLeastOneIndexed) {
                    return false;
                }


            }

            for (int i = 0; i < logicalOpers.size() - 1; i++) {


                var left = ((String)logicalOpers.get(i)[0] );
                var next = ((String)logicalOpers.get(i+1)[0] );

                int leftOrIdx = ((Integer)logicalOpers.get(i)[1] );

                if(left.equalsIgnoreCase("OR") && next.equalsIgnoreCase("OR")){

                    var middleLeft = tokens.get(leftOrIdx + 3);
                    var middleOperator = tokens.get(leftOrIdx + 2);
                    var middleRight = tokens.get(leftOrIdx + 1);

                    var LeftExpression = new String[]{middleLeft, middleOperator, middleRight};

                    // check that first token is a column name and the third is a value
                    var firstIsColumnName = table.AttribIdxs.containsKey(LeftExpression[0]);
                    var thirdIsColumnName = table.AttribIdxs.containsKey(LeftExpression[2]);


                    // if both true or both false then bad
                    if (firstIsColumnName == thirdIsColumnName) {
                        return false;
                    }

                    // test that at least one is indexed

                    boolean atLeastOneIndexed = isLegalIndexExpr(LeftExpression, table);


                    // failure to have and exploit indexing

                    if (!atLeastOneIndexed) {
                        return false;
                    }

                }

            }

        }

            return true;
    }


    public static boolean isLegalIndexExpr(String[] tokens, Table table) {


        // check that first token is a column name and the third is a value
        var firstIsColumnName = table.AttribIdxs.containsKey(tokens[0]);
        var thirdIsColumnName = table.AttribIdxs.containsKey(tokens[2]);


        // if both true or both false then bad
        if (firstIsColumnName == thirdIsColumnName) {
            return false;
        }


        HashSet<String> operators = new HashSet<>(List.of(new String[]{"=", ">", ">=", "<", "<=", "!="}));

        //  second token should be an operator
        var operator = operators.contains(tokens[1]);

        if (!operator) {
            return false;
        }

        // check if one of the atributes is indexed


        return table.IndexedAttributes.containsKey(tokens[0]) || table.IndexedAttributes.containsKey(tokens[2]);

    }

    public static ArrayList<RecordPointer> GetCase2Recs(List<String> tokens, Table table) {




        ArrayList<RecordPointer> rps = new ArrayList<>();
        // for ands we and to get the expression to the left and right

        // we are looking for adjacent ors like   or expr or



        ArrayList<Integer> expressionSepIdx = new ArrayList<>();

        for (int i = 0; i < tokens.size(); i++) {
            if (tokens.get(i).equalsIgnoreCase("OR") || tokens.get(i).equalsIgnoreCase("AND")) {
                expressionSepIdx.add(i);

            }
        }

        // get all the expressions
        int start = 0;
        ArrayList<List<String>> expressions = new ArrayList<>();

        for (int end: expressionSepIdx) {

            var exp = tokens.subList(start,end);
            expressions.add(new ArrayList<>(exp));
            start = end + 1;
        }
        var exp = tokens.subList(start,tokens.size());
        expressions.add(new ArrayList<>(exp));


        // get the indexs on the ones that we can

        System.err.println(expressions);

        for (var expr: expressions) {
            // if the expression has an index grab the recs
            var exprArr = new String[] {expr.get(0),expr.get(1),expr.get(2)};
            if(isLegalIndexExpr(exprArr,table)){
                System.err.println(expr);

                // get record pointers for the condition on the index

                var thirdIsColumnName = table.AttribIdxs.containsKey(exprArr[2]);
                // if both true or both false then bad
                var attributeName = exprArr[0];
                var op = exprArr[1];
                var value = exprArr[2];

                if (thirdIsColumnName) {
                     attributeName = exprArr[0];
                     value = exprArr[2];
                }


                BPlusTree currTree = table.IndexedAttributes.get(attributeName);
                var recs = StorageManager.GetRecsFromTreeWhere( currTree,  op,  value);


                // todo add rects to total and return them removing dups


            }

        }







        return rps;
    }

    /*--------------    HOW TO USE  -------------------


    ...
            */
    public boolean whereIsTrue(String whereStmt, Table table, ArrayList<Object> row) {

        List<String> tokens = tokenizer(whereStmt, table, row);

        if (tokens == null) {
            return false;
        }
        return Validate(tokens);
    }


    /////////////////////////////////////////// EXAMPLE
    public static void main(String[] args) {


        Catalog.createCatalog("DB", 120, 3);
        StorageManager.createStorageManager();
        AStorageManager sm = StorageManager.getStorageManager();
        Catalog cat = (Catalog) Catalog.getCatalog();
        WhereP3 parser = new WhereP3();


        // TABLES
        //t1
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("t1.a", "Integer"));
        attrs.add(new Attribute("t1.uidt1", "Integer"));
        var t1 = cat.addTable("t1", attrs, attrs.get(0));


        //t2
        ArrayList<Attribute> attrs2 = new ArrayList<>();
        attrs2.add(new Attribute("t2.b", "Char(5)"));
        attrs2.add(new Attribute("t2.c", "Char(5)"));
        attrs2.add(new Attribute("t2.uidt2", "Char(5)"));
        var t2 = cat.addTable("t2", attrs2, attrs2.get(0));

        //t3
        ArrayList<Attribute> attrs3 = new ArrayList<>();
        attrs3.add(new Attribute("t3.a", "Char(5)"));
        attrs3.add(new Attribute("t3.uidt3", "Char(5)"));
        var t3 = cat.addTable("t3", attrs3, attrs3.get(0));


        // cartesian product table

        // 1) adding the attributes from all the tables together
        ArrayList<Attribute> catAt = new ArrayList<>();
        catAt.addAll(attrs);
        catAt.addAll(attrs2);
        catAt.addAll(attrs3);


        // add cartesian table
        Table catTab = (Table) Catalog.getCatalog().addTable("catTab", catAt, catAt.get(0));

//        System.out.println(catAt.get(0));

        //TODO FOR CAT TABLE IS ADD ALL TABLES INDEX ATTRIBUTES TOGETHER

        // add an index on attribute

        System.out.println(catTab.IndexedAttributes.get("t1.a"));

        catTab.addIndex("t1.uidt1");

        System.out.println(catTab.IndexedAttributes.get("t1.a"));


        // add some records to cartesian table
        Random rand = new Random();
        int bound = 100000;
        int size = 10;

        for (int i = 0; i < size; i++) {
            var row = Phase2Testers.mkRandomRec(catAt);
//          row.set(0, rand.nextInt(bound));
            row.set(1, size - (i));
            System.out.print("INSERTING (#" + i + ") " + " " + row + " :");
            boolean b = StorageManager.getStorageManager().insertRecord(catTab, row);
            System.out.println(b);

        }

        catTab.getPkTree().print();
        catTab.IndexedAttributes.get("t1.uidt1").print();

        prettyPrintTable(catTab);
        System.out.println();
        System.out.println("DONE INSERTING CURRENT TREES");


        var row = Phase2Testers.mkRandomRec(catAt);
        row.set(0, 5);
        System.out.print("DELETING (#" + 1 + ") " + " " + row + " :");
        boolean b = StorageManager.getStorageManager().deleteRecord(catTab, row.get(catTab.pkIdx()));
        System.out.println(b);

        catTab.getPkTree().print();
        catTab.IndexedAttributes.get("t1.uidt1").print();

        prettyPrintTable(catTab);


        System.out.println("indexs on :" + catTab.IndexedAttributes.keySet());


//        System.exit(1);


        long startTime = System.currentTimeMillis();
//
//
//        var res = ((StorageManager) sm).getWhere(catTab, "where t1.a != 2 or t1.a != 2 or t1.uidt1 < 10 and t1.a != 2 or t1.a != 2 AND t3.uidt3 < 20 or t1.uidt1 < 5");
        var res = ((StorageManager) sm).getWhere(catTab, "where 2 != t1.a AND t2.b != 2 AND t1.a != 2");

        long endTime = System.currentTimeMillis();

        System.out.println("STMT: " + res);
        System.out.println(endTime - startTime);

        // if we have an

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


