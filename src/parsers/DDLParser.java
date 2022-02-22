package parsers;

import common.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
  Class for DDL parser

  This class is responsible for parsing DDL statements

  You will implement the parseDDLStatement function.
  You can add helper functions as needed, but the must be private and static.

  @author Scott C Johnson (sxjcs@rit.edu)

 */
public class DDLParser {

    /**
     * This function will parse and execute DDL statements (create table, create index, etc)
     * @param stmt the statement to parse
     * @return true if successfully parsed/executed; false otherwise
     */

    public static Set<String> KEYWORDS =  Stream.of(
                    "create", "table", "drop","insert","into","delete","from","where","update",
                    "notnull","primarykey","foreignkey","references","add","default","set",
                    "values").collect(Collectors.toCollection(HashSet::new));

    public static boolean parseDDLStatement(String stmt){


        if (stmt.toLowerCase().startsWith("create table")){
            return CreateTable(stmt);
        }




        return false;
    }



    //TODO
    // --- Names can start with a alpha-character and contain alphanumeric characters
    // --- how many attrib Constraints can attib have??
    // --- The referenced table must exist and the data types of the attributes must match.
    private static boolean CreateTable(String stmt){


        stmt = stmt.substring(13);
        stmt = stmt.replace("\n","");
        String TableName = stmt.substring(0,stmt.indexOf("("));
        stmt = stmt.substring(TableName.length()+1);
        if (KEYWORDS.contains(TableName.toLowerCase())){
            System.err.println("table name: "+TableName+" is a keyword");
            return false;
        }
        System.out.println(TableName);
        boolean tableHasPk = false;

        ArrayList<Attribute> TableAttributes = new ArrayList<>();
        Attribute primaryKey = null;
        ArrayList<Attribute> TableForeignkeys = new ArrayList<>();



        for(String attrib: stmt.split(",")) {
            String attribute = attrib.strip();
            String UppercaseAttribute = attribute.toUpperCase();

            if (UppercaseAttribute.startsWith("PRIMARYKEY")) {

                // check for more then 1 pk
                if(tableHasPk){
                    System.err.println("creating table "+TableName+" cant have more then 1 primary key");
                    return false;
                }

                // regex pk name out
                String pk = attribute.substring(11).replace(")","").strip();

                // check for keyword
                if (KEYWORDS.contains(pk.toLowerCase())){
                    System.err.println("PRIMARYKEY name: "+pk+" is a keyword");
                    return false;
                }

                System.out.println("primarykey: {" + pk+"}");
                tableHasPk = true;

            } else if (UppercaseAttribute.startsWith("FOREIGNKEY")) {

                // regex names out
                String fk = attribute.replaceAll("(?i)(foreignkey|references)","").replace(";","");
                fk = fk.replace(" ", "").replaceAll("[()]"," ").strip();

                //attrib -> tablename attrib
                String[] fkSpit = fk.split(" ");

                // check for keyword
                if (KEYWORDS.contains(fkSpit[0].toLowerCase())){
                    System.err.println("foreignkey name: "+fkSpit[0]+" is a keyword");
                    return false;
                }

                System.out.println(Arrays.toString(fkSpit));

            } else {
                //regex names types and Constraints out
                String[] AttributeSplit = attribute.split(" ");
                String AttributeName = AttributeSplit[0];
                String AttributeType = AttributeSplit[1].toUpperCase();
                ArrayList<String> AttributeConstraint = new ArrayList<>();

                // check for keyword
                if (KEYWORDS.contains(AttributeName.toLowerCase())){
                    System.err.println("Attribute name: "+AttributeName+" is a keyword");
                    return false;
                }

                // check for Attribute Constraints
                if(AttributeSplit.length>2){
                    AttributeConstraint.addAll(Arrays.asList(AttributeSplit).subList(2, AttributeSplit.length));
                    // check for keywords allowed
                    AttributeConstraint.removeIf(constraint -> !constraint.equalsIgnoreCase("notnull") &&  !constraint.equalsIgnoreCase("primarykey"));
                }
                System.out.println("attribute NAME: {" + AttributeName+"} TYPE: {"+AttributeType+
                        "} CONSTRAINTS: "+AttributeConstraint);
            }
        }

        // TODO MAKE TABLE AND CHECK that everything in the todo above is all good
        // check pk is in the attributes for this table

        // add table to catalog

        // TODO IN table make sure to add fk write out and not null indexes to table





        return false;
    }


    public static void main(String[] args) {
        CreateTable("""
                creAte taBle f0o(
                        bAz integer,
                        baR Double notnull,
                        primarykey(bar1),
                        foreignkey( bar3 ) REFERENCES bazzle1( baz6 ),
                        FOREIGNKEY(bar4) references bazzle2( baz7 ),
                        foreignkey(bar) references bazzle3(baz8)
                );""");
    }
}
