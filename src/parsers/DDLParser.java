package parsers;

import java.util.ArrayList;
import java.util.Arrays;

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
    public static boolean parseDDLStatement(String stmt){


        if (stmt.startsWith("create table")){
            return CreateTable(stmt);
        }




        return false;
    }



    //TODO
    // --- pk can only have one
    // --- things cant be named Keyword
    // --- Names can start with a alpha-character and contain alphanumeric characters
    // --- how many attrib Constraints can attib have??
    // --- The referenced table must exist and the data types of the attributes must match. foreignkey and references are key words.
    private static boolean CreateTable(String stmt){


        stmt = stmt.substring(13);
        stmt = stmt.replace("\n","");
        String TableName = stmt.substring(0,stmt.indexOf("("));
        stmt = stmt.substring(TableName.length()+1);

        System.out.println(TableName);

        for(String attrib: stmt.split(",")) {
            String attribute = attrib.strip();
            String UppercaseAttribute = attribute.toUpperCase();

            if (UppercaseAttribute.startsWith("PRIMARYKEY")) {
                String pk = attribute.substring(11).replace(")","").strip();
                System.out.println("primarykey: {" + pk+"}");
            } else if (UppercaseAttribute.startsWith("FOREIGNKEY")) {

                String fk = attribute.replaceAll("(?i)(foreignkey|references)","").replace(";","");
                fk = fk.replace(" ", "").replaceAll("[()]"," ").strip();
                String[] fkSpit = fk.split(" ");
                System.out.println(Arrays.toString(fkSpit));

            } else {
                String[] AttributeSplit = attribute.split(" ");
                String AttributeName = AttributeSplit[0];
                String AttributeType = AttributeSplit[1].toUpperCase();
                ArrayList<String> AttributeConstraint = new ArrayList<>();

                if(AttributeSplit.length>2){
                    AttributeConstraint.addAll(Arrays.asList(AttributeSplit).subList(2, AttributeSplit.length));
                }
                System.out.println("attribute NAME: {" + AttributeName+"} TYPE: {"+AttributeType+
                        "} CONSTRAINTS: "+AttributeConstraint);
            }
        }

        // TODO MAKE TABLE AND CHECK that everything in the todo above is all good

        // TODO IN table make sure to add fk write out and not null indexes to table





        return false;
    }


    public static void main(String[] args) {
        CreateTable("""
                creAte taBle f0o(
                        bAz integer,
                        baR Double notnull,
                        primarykey(bar1),
                        PRIMARYKEY( bar2 ),
                        foreignkey( bar3 ) REFERENCES bazzle1( baz6 ),
                        FOREIGNKEY(bar4) references bazzle2( baz7 ),
                        foreignkey(bar5) references bazzle3( baz8 )
                );""");
    }
}
