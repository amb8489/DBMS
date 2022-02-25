package parsers;


import common.Attribute;

import java.util.*;
import java.util.regex.Pattern;

import static java.util.Map.entry;

public class WhereParser {

    private static HashMap<String, Tuple<List<String>, ArrayList<Tuple<Integer, Integer>>>> CacheWhereStmtPlacementPattern = new HashMap<>();

    private static final Map<String, Integer> precedence = Map.ofEntries(
            entry("and", 1), entry("or", 2),
            entry("=", 3), entry("!=", 3),
            entry("<", 3), entry(">", 3),
            entry("<=", 3), entry(">=", 3));

    private static final Set operators = precedence.keySet();


    private static boolean Validate(List<String> tokens, List<Object> row) {


        try {
            // making stacks for shunting yard algo

            Stack<String> stack = new Stack<>();
            List<Object> Output = new ArrayList<>();

            // look through each char
            for (String token : tokens) {

                boolean OpsContains = operators.contains(token);
                boolean isLeftParentheses = token.equals("(");
                boolean isRightParentheses = token.equals(")");


                if (!OpsContains && !isLeftParentheses && !isRightParentheses) {
                    Output.add(token);

                } else if (OpsContains) {
                    if (!stack.isEmpty()) {
                        String top = stack.peek();
                        Integer tokenPrec = precedence.get(token);


                        while (!stack.isEmpty() && !top.equals("(") &&
                                operators.contains(top) && tokenPrec <= precedence.get(top)) {

                            String t = stack.pop();
                            Output.add(t);

                            if (Output.size() >= 3 && operators.contains(t)) {
                                Object val = eval(Output.get(Output.size() - 3), Output.get(Output.size() - 2), Output.get(Output.size() - 1));
                                Output = Output.subList(0, Output.size() - 3);
                                Output.add(val);

                            }

                            if (!stack.isEmpty()) {
                                top = stack.peek();
                            }
                        }
                    }

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


            while (!stack.isEmpty()) {
                String t = stack.pop();
                Output.add(t);

                if (Output.size() >= 3 && operators.contains(t)) {
                    Object val = eval(Output.get(Output.size() - 3), Output.get(Output.size() - 2), Output.get(Output.size() - 1));
                    Output = Output.subList(0, Output.size() - 3);
                    Output.add(val);
                }
            }
            return (boolean) Output.get(Output.size() - 1);
        } catch (Exception exp) {
            System.err.println(exp);
            return false;
        }
    }


    private static Object eval(Object lexp, Object rexp, Object oplexp) throws Exception {

        String left = lexp.toString();
        String right = rexp.toString();
        String op = oplexp.toString();


        switch (op) {
            case "and":
                return left.equals("true") && right.equals("true");
            case "or":
                return left.equals("true") || right.equals("true");
            default:
                break;
        }


        boolean leftMatch = false;
        boolean rightMatch = false;


        if (left.equals(".")) {
            leftMatch = true;

        } else {

            for (int i = 0; i < left.length(); i++) {
                char ch = left.charAt(i);
                if (ch > '9' || ch < '.' || ch == '/') {
                    leftMatch = true;
                    break;
                }
            }
        }

        if (right.equals(".")) {
            rightMatch = true;

        } else {

            for (int i = 0; i < right.length(); i++) {
                char ch = right.charAt(i);
                if (ch > '9' || ch < '.' || ch == '/') {
                    rightMatch = true;
                    break;
                }
            }
        }
        if (leftMatch && rightMatch) {

            return switch (op) {
                case "=" -> left.compareTo(right) == 0;
                case "!=" -> left.compareTo(right) != 0;
                case "<" -> left.compareTo(right) < 0;
                case ">" -> left.compareTo(right) > 0;
                case "<=" -> left.compareTo(right) <= 0;
                case ">=" -> left.compareTo(right) >= 0;
                default -> null;
            };
        } else if (!leftMatch && !rightMatch) {
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
        throw new Exception("COMPARING DIFFERENT TYPES:" + left + " with " + right);

    }


    private static List<String> fillString(String s, List<Object> r, ArrayList<Attribute> attrs) {

        if (!CacheWhereStmtPlacementPattern.containsKey(s)) {

            String stmt = s;

            s = s.replace("(", " ( ");
            s = s.replace(")", " ) ");
            s = s.replace("=", " = ");
            s = s.replace("!=", " != ");
            s = s.replace("<", " < ");
            s = s.replace(">", " > ");
            s = s.replace("<=", " <= ");
            s = s.replace(">=", " >= ");


            List<String>tokens = StringFormatter.mkTokensFromStr(s);

            int whereIdx = 1;
            for (String t : tokens) {
                if (t.equalsIgnoreCase("where")) {
                    tokens = tokens.subList(whereIdx, tokens.size());
                    break;
                }
                whereIdx++;
            }


            HashMap<String, Integer> AttribNames = new HashMap<>();
            for (int i = 0; i < attrs.size(); i++) {
                AttribNames.put(attrs.get(i).getAttributeName(), i);
            }


            // loop though tokens

            int tokenIdx = 0;
            ArrayList<Tuple<Integer, Integer>> IdxsToReplace = new ArrayList<>();
            for (String t : tokens) {
                // if that token is a column name in the table, (aka an attribute name)
                if (AttribNames.containsKey(t)) {
                    // set that token to the value from the given row at the idx of the col name
                    Integer attribIdx = AttribNames.get(t);

                    IdxsToReplace.add(new Tuple<>(tokenIdx, attribIdx));

                    tokens.set(tokenIdx, r.get(attribIdx).toString());
                }
                tokenIdx++;
            }
            Tuple<List<String>, ArrayList<Tuple<Integer, Integer>>> tup = new Tuple<>(tokens, IdxsToReplace);
            CacheWhereStmtPlacementPattern.put(stmt, tup);
            return tokens;
        }


        Tuple<List<String>, ArrayList<Tuple<Integer, Integer>>> cached = CacheWhereStmtPlacementPattern.get(s);
        List<String> tokens = cached.x;
        ArrayList<Tuple<Integer, Integer>> replaceAt = cached.y;
        for (Tuple<Integer, Integer> t : replaceAt) {
            tokens.set(t.x, r.get(t.y).toString());
        }

        return tokens;
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


    if (whereIsTrue(stmt, row attribs)){
        delete record
    }
    ...
            */
    public boolean whereIsTrue(String stmt, List<Object> row, ArrayList<Attribute> attrs) {
        List<String> tokens = fillString(stmt, row, attrs);
        return Validate(tokens, row);
    }


    /////////////////////////////////////////// EXAMPLE
    public static void main(String[] args) {


        WhereParser parser = new WhereParser();


        // TALE ATTRIBUTES
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("fName", "Varchar(10)"));
        attrs.add(new Attribute("lName", "Varchar(10)"));
        attrs.add(new Attribute("age", "Integer"));
        attrs.add(new Attribute("gpa", "Double"));
        attrs.add(new Attribute("ID", "Integer"));
        Attribute pk = attrs.get(4);


        // ROW VALS
        List<Object> r = new ArrayList<>();
        r.add("\"Aaron\"");
        r.add("Berghash");
        r.add(23);
        r.add(3.4);
        r.add(1);

        // STMT
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            String s = "delete from foo where gpa < 1 or Fname = berg";

            boolean res = parser.whereIsTrue(s, r, attrs);
//            System.out.println("STMT IS :" + res);
            r.set(3, i);
        }

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
}


