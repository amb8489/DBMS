package parsers;


import common.Attribute;

import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static java.util.Map.entry;

public class WhereParser {

    private Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");

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


    private static boolean Validate(List<String> tokens, List<Object> row) {

        try {
            // making stacks for shunting yard algo

            Stack<String> stack = new Stack<String>();
            List<Object> Output = new ArrayList<Object>();

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
                                List<Object> nd = Output.subList(Output.size() - 3, Output.size());
                                Output = Output.subList(0, Output.size() - 3);
                                Output.add(eval(nd));
                                System.out.println("  " + Output.get(Output.size() - 1));

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
                            List<Object> nd = Output.subList(Output.size() - 3, Output.size());
                            Output = Output.subList(0, Output.size() - 3);
                            Output.add(eval(nd));
                            System.out.println("  " + Output.get(Output.size() - 1));

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
                    List<Object> nd = Output.subList(Output.size() - 3, Output.size());
                    Output = Output.subList(0, Output.size() - 3);
                    Output.add(eval(nd));
                    System.out.println("  " + Output.get(Output.size() - 1));
                }
            }
            return (boolean) Output.get(Output.size() - 1);
        }catch (Exception exp) {
            System.err.println(exp);
            return false;
        }
    }


    /*


    TODO refactor this junk using the types given in the attribs in fillString
     */
    private static Object eval(List<Object> nd) throws Exception {
        System.out.print("eval: " + nd);

        String left = nd.get(0).toString();
        String right = nd.get(1).toString();
        String op = nd.get(2).toString();


        switch (op) {
            case "and":
                return left.equals("true") && right.equals("true");
            case "or":
                return left.equals("true") || right.equals("true");
            default:
                break;
        }

        if (left.matches("^.*[A-Za-z].*$") && right.matches("^.*[A-Za-z].*$")) {
            return switch (op) {
                case "=" -> left.compareTo(right) == 0;
                case "!=" -> left.compareTo(right) != 0;
                case "<" -> left.compareTo(right) < 0;
                case ">" -> left.compareTo(right) > 0;
                case "<=" -> left.compareTo(right) <= 0;
                case ">=" -> left.compareTo(right) >= 0;
                default -> null;
            };
        } else if (!left.matches(".*[A-Za-z].*$") && !right.matches("^.*[A-Za-z].*$")) {

            Double numLeft = Double.parseDouble(left);
            Double numRight = Double.parseDouble(right);

            return switch (op) {
                case "=" -> numLeft.compareTo(numRight) == 0;
                case "!=" -> numLeft.compareTo(numRight) != 0;
                case "<" -> numLeft.compareTo(numRight) < 0;
                case ">" -> numLeft.compareTo(numRight) > 0;
                case "<=" -> numLeft.compareTo(numRight) <= 0;
                case ">=" -> numLeft.compareTo(numRight) >= 0;
                default -> null;
            };
        }
        throw new Exception("\"COMPARING DIFFERENT TYPES:\" + left + \" with \" + right");

    }


    /*




     */
    private static List<String> fillString(String s, List<Object> r, ArrayList<Attribute> attrs) {
        s = s.replace("(", "( ");
        s = s.replace(")", " )");
        s = s.replace("=", " = ");
        s = s.replace("!=", " != ");
        s = s.replace("<", " < ");
        s = s.replace(">", " > ");
        s = s.replace("<=", " <= ");
        s = s.replace(">=", " >= ");


        List<String> tokens = new ArrayList<>(List.of(s.split(" ")));
        System.out.println(tokens);

        tokens.removeIf(String::isBlank);
        System.out.println(tokens);
        int idx = 0;
        for (Attribute a : attrs) {
//            int i = tokens.indexOf(a.attributeName());
            int[] WhereColEqName = IntStream.range(0, tokens.size()).filter(i -> tokens.get(i).equals(a.attributeName())).toArray();

            System.out.println(Arrays.toString(WhereColEqName));

            for(Integer i :WhereColEqName) {
                String val = r.get(idx).toString();
                tokens.set(i, val);
            }
            idx += 1;
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

    1) you will pass in ONLY what comes after where and removing the semi-colon at the end:
       "(Fname = Aaron and Gpa < 3) or HeightI = 71"

    2) the row values (aaron,berg,3.4,71,23)

    3) the attribs for that table (Fname,Lname,Gpa,HeightI,Age)

    ...
    if (whereIsTrue(stmt, row attribs)){
        delete record
    }
    ...

    //TODO
       - ASK about () in stmts
       - ASK about "" for strings
       - possible refactor of this entire thing to better accommodate eval function
     */
    public static boolean whereIsTrue(String stmt, List<Object> row, ArrayList<Attribute> attrs) {
        List<String> tokens = fillString(stmt, row, attrs);
        return Validate(tokens, row);
    }



    /////////////////////////////////////////// EXAMPLE
    public static void main(String[] args) {

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

        String s = "(fName=\"AArON\" or gpa>2.0) and (lName=berg or 2 < 2) or gpa > 1";
        System.out.println(s);
        System.out.println("STMT IS :"+whereIsTrue(s, r, attrs));

    }


}
