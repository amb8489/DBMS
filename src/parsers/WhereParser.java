package parsers;


import common.Attribute;
import common.Utilities;

import java.util.*;

import static java.util.Map.entry;

public class WhereParser {


    // a cache for already seen statements
    private static HashMap<String, Tuple<List<String>, ArrayList<Tuple<Integer, Integer>>>> CacheWhereStmtPlacementPattern = new HashMap<>();

    // used in the shunting yard algo list of operators and their precedence
    private static final Map<String, Integer> precedence = Map.ofEntries(
            entry("and", 1), entry("or", 2),
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


                            // eval if we can
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
                // eval as we go
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
        switch (op) {
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
        boolean leftIsStr = false;
        boolean rightIsStr = false;

        Double numLeft = 0.0;
        Double numRight = 0.0;


        // checking left val type
        if (left.startsWith("\"")) {
            leftIsStr = true;

        } else {

            try {
                Double.parseDouble(left);
            } catch (Exception e) {
                leftIsStr = true;
            }
        }


        // checking right val type
        if (right.startsWith("\"")) {
            rightIsStr = true;

        } else {

            try {
                Double.parseDouble(right);
            } catch (Exception e) {
                rightIsStr = true;
            }
        }

        // if both vals are strings
        if (leftIsStr && rightIsStr) {

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
        } else if (!rightIsStr && !leftIsStr) {
            numLeft = Double.parseDouble(left);
            numRight = Double.parseDouble(right);

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
        throw new Exception("COMPARING DIFFERENT TYPES:" + left + " with " + right);

    }

    // TODO check attributes exits when adding

    // fill string will get a string ready to be run through the validate function
    // string needs to be formated correctly and then be split into tokens
    private static List<String> tokenizer(String s, List<Object> r, ArrayList<Attribute> attrs) {

        // first thing we do is look to see if weve seen this stmt before
        // if we have then we have cached the work we did on the string the first time we tokenized it
        // and dont need to do all that work again
        if (!CacheWhereStmtPlacementPattern.containsKey(s)) {

            // save original stmt for caching
            String stmt = s;

            // make sure we have spacing between operators and values
            s = s.replace("(", " ( ");
            s = s.replace(")", " ) ");

            s = s.replace("!", " !");
            s = s.replace("<", " < ");
            s = s.replace(">", " > ");
            s = s.replace("=", " = ");

            s = s.replace("<  =", " <= ");
            s = s.replace(">  =", " >= ");
            s = s.replace("! =", " != ");

            // tokenize the string by spaces
            List<String> tokens = Utilities.mkTokensFromStr(s);


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


            // mapping the attribute name to the idx of that attribute  needed for later
            HashMap<String, Integer> AttribNames = new HashMap<>();
            for (int i = 0; i < attrs.size(); i++) {
                AttribNames.put(attrs.get(i).getAttributeName(), i);
            }


            // loop though tokens
            int tokenIdx = 0;

            //todo make sure attribute is actally in the table if its on the right or left of operator like bazzel in write up

            // what we are looking for is the token to match with the attribute name
            // if it does then we replace it with the value in the row at that idx
            // this is what is being cached
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
            // caching
            Tuple<List<String>, ArrayList<Tuple<Integer, Integer>>> tup = new Tuple<>(tokens, IdxsToReplace);
            CacheWhereStmtPlacementPattern.put(stmt, tup);

            // return tokens ready to go to the validator
            return tokens;
        } else {

            // getting cached vals token idx to be replaced with idx in row
            Tuple<List<String>, ArrayList<Tuple<Integer, Integer>>> cached = CacheWhereStmtPlacementPattern.get(s);
            List<String> tokens = cached.x;
            ArrayList<Tuple<Integer, Integer>> replaceAt = cached.y;

            // doing the repayment
            for (Tuple<Integer, Integer> t : replaceAt) {
                tokens.set(t.x, r.get(t.y).toString());
            }


            return tokens;
        }
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
    public boolean whereIsTrue(String stmt, List<Object> row, ArrayList<Attribute> attrs) {
        List<String> tokens = tokenizer(stmt, row, attrs);
        return Validate(tokens);
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

        for (int i = 0; i < 10; i++) {
            String s = "delete from foo where gpa < 1 or fName = \"berg\"";

            boolean res = parser.whereIsTrue(s, r, attrs);
            System.out.println("STMT: " + res);
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


