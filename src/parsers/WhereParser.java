package parsers;


import common.Attribute;

import java.util.*;

import static java.util.Map.entry;

public class WhereParser {

    private static final Map<String, Integer> precedence = Map.ofEntries(
            entry("and", 1),
            entry("or", 2),
            entry("=", 3),
            entry("!=", 3),
            entry("<", 3),
            entry(">", 3),
            entry("<=", 3),
            entry(">=", 3));

    private static final Set<String> operators = precedence.keySet();


    /*--------------    HOW TO USE  -------------------


    stmt is a string that follows the where clause.
    EXAMPLE:
    if your statement was:
    "delete from foo where foo > 2 or 1 = 1 and doo != 2 or 21 = 21;"

    -- you will pass in ONLY: "[foo > 2 or 1 = yoo and doo != 2 or 21 = 21"

   1) REMOVE ANYTHING BEFORE AND INCLUDING THE WHERE
   2) REMOVE THE ; AT THE END

     */
    private static boolean Validate(List<String> tokens, List<Object> row) {



        // making stacks for shunting yard algo

//        List<String> q = new ArrayList<String>();
        Stack<String> stack = new Stack<String>();
        List<Object> Output = new ArrayList<Object>();

        // look through each char
        for (String token : tokens) {

            boolean OpsContains = operators.contains(token);
            boolean isLeftParentheses = token.equals("(");
            boolean isRightParentheses = token.equals(")");


            if (!OpsContains && !isLeftParentheses && !isRightParentheses) {
//                q.add(token);
                Output.add(token);

            } else if (OpsContains) {
                if (!stack.isEmpty()) {
                    String top = stack.peek();
                    Integer tokenPrec = precedence.get(token);


                    while (!stack.isEmpty() && !top.equals("(") &&
                            operators.contains(top) && tokenPrec <= precedence.get(top)) {

                        String t = stack.pop();
//                        q.add(t);
                        Output.add(t);
                        if (Output.size() >= 3 && operators.contains(t)) {
                            List<Object> nd = Output.subList(Output.size() - 3, Output.size());
                            Output = Output.subList(0, Output.size() - 3);
                            Output.add(eval(nd));
                            System.out.println("--------------------|"+Output.get(Output.size()-1));

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
//                    q.add(t);
                    Output.add(t);
                    if (Output.size() >= 3 && operators.contains(t)) {
                        List<Object> nd = Output.subList(Output.size() - 3, Output.size());
                        Output = Output.subList(0, Output.size() - 3);
                        Output.add(eval(nd));
                        System.out.println("--------------------|"+Output.get(Output.size()-1));

                    }
                }

                if (!stack.isEmpty() && stack.peek().equals("(")) {
                    stack.pop();
                }
            }
        }


        while (!stack.isEmpty()) {
            String t = stack.pop();
//            q.add(t);
            Output.add(t);
            if (Output.size() >= 3 && operators.contains(t)) {
                List<Object> nd = Output.subList(Output.size() - 3, Output.size());
                Output = Output.subList(0, Output.size() - 3);
                Output.add(eval(nd));
                System.out.println("--------------------|"+Output.get(Output.size()-1));
            }
        }
        return (boolean) Output.get(Output.size() - 1);
    }

    private static Object eval(List<Object> nd) {
        System.out.println("eval--------------->" + nd);
        String left = nd.get(0).toString();
        String right = nd.get(1).toString();
        String op = nd.get(2).toString();

        switch (op) {
            case "and":
                return left.equals("true") && right.equals("true");
            case "or":
                return left.equals("true") || right.equals("true");
            case "=":
                return Objects.equals(left, right);
            case "!=":
                return !Objects.equals(left, right);
            case "<":
                return left.compareTo(right) < 0;
            case ">":
                return left.compareTo(right) > 0;
            case "<=":
                return left.compareTo(right) <= 0;
            case ">=":
                return left.compareTo(right) >= 0;
            default:
                return null;
        }

    }

    private static List<String> fillString(String s, List<Object> r, ArrayList<Attribute> attrs) {
        s = s.replace("(","( ");
        s = s.replace(")"," )");

        List<String> tokens = new ArrayList<>(List.of(s.split(" ")));
        int idx = 0;
        for (Attribute a : attrs) {
            int i = tokens.indexOf(a.attributeName());
            if (i != -1) {
                String val = r.get(idx).toString();
                tokens.set(i, val);
            }
            idx += 1;
        }
        return tokens;
    }





    public static boolean whereIsTrue(String stmt, List<Object> row, ArrayList<Attribute> attrs){
        List<String> tokens = fillString(stmt, row, attrs);
        return Validate(tokens, row);
    }

    public static void main(String[] args) {

        // mk table
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(new Attribute("fName", "Varchar(10)"));
        attrs.add(new Attribute("lName", "Varchar(10)"));
        attrs.add(new Attribute("age", "Integer"));
        attrs.add(new Attribute("gpa", "Double"));
        attrs.add(new Attribute("ID", "Integer"));
        Attribute pk = attrs.get(4);

        List<Object> r = new ArrayList<>();
        r.add("\"Aaron\"");
        r.add("Berghash");
        r.add(23);
        r.add(3.4);
        r.add(1);

        String s = "fName = \"Aaron\" and gpa > 2 and lName = berg and 1 = 1";

        System.out.println(whereIsTrue(s,r,attrs));

    }


}
