package parsers;

import catalog.Catalog;
import common.Attribute;
import common.ForeignKey;
import common.Table;
import common.VerbosePrint;

import java.nio.ByteBuffer;
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

  @author Aaron Berghash
  @author Emma Reynolds
  @author Scott C Johnson (sxjcs@rit.edu)

 */
public class DDLParser {

    /**
     * This function will parse and execute DDL statements (create table, create index, etc)
     *
     * @param stmt the statement to parse
     * @return true if successfully parsed/executed; false otherwise
     */

    private static Set<String> KEYWORDS = Stream.of(
            "create", "table", "drop", "insert", "into", "delete", "from", "where", "update",
            "notnull", "primarykey", "foreignkey", "references", "add", "default", "set",
            "values", "null").collect(Collectors.toCollection(HashSet::new));

    public static boolean parseDDLStatement(String stmt) {



        if (stmt.toLowerCase().startsWith("create table")) {
            return CreateTable(stmt);
        }else if (stmt.toLowerCase().startsWith("drop table")) {
            return dropTable(stmt);
        }else if (stmt.toLowerCase().startsWith("alter table")) {
            return alterTable(stmt);
        }

        return false;
    }


    //TODO
    // --- how many attrib Constraints can attib have write up inconsistent??
    private static boolean CreateTable(String stmt) {
        try {


            // vars for the new table
            ArrayList<Attribute> TableAttributes = new ArrayList<>();
            Attribute primaryKey = null;
            ArrayList<ForeignKey> TableForeignkeys = new ArrayList<>();
            ArrayList<Integer> notNullIndexs = new ArrayList<>();

            String TableName;


            //-----------------find the table name key-----------------
            stmt = stmt.substring(13);
            stmt = stmt.replace("\n", "");
            TableName = stmt.substring(0, stmt.indexOf("("));
            stmt = stmt.substring(TableName.length() + 1);

            // check that name is not keyword
            if (isiIllLegalName(TableName.toLowerCase())) {
                return false;
            }
            VerbosePrint.print("table name" + TableName);

            // parsing for the rest of the string
            boolean tableHasPk = false;
            int numberOfNewAttribs = 0;
            for (String attrib : stmt.split(",")) {
                String attribute = attrib.strip();
                String UppercaseAttribute = attribute.toUpperCase();


                //-----------------find the PRIMARY key-----------------
                if (UppercaseAttribute.startsWith("PRIMARYKEY")) {

                    // check for more than 1 pk
                    if (tableHasPk) {
                        System.err.println("creating table " + TableName + " cant have more then 1 primary key");
                        return false;
                    }

                    // regex pk name out
                    String pk = attribute.substring(11).replace(")", "").strip();

                    // check for name keyword
                    if (isiIllLegalName(pk.toLowerCase())) {
                        return false;
                    }
                    VerbosePrint.print("primarykey: {" + pk + "}");

                    for (Attribute att : TableAttributes) {
                        if (att.getAttributeName().equals(pk)) {
                            primaryKey = att;
                            tableHasPk = true;
                            break;
                        }
                    }
                    if (!tableHasPk) {
                        System.err.println("table does not have attribute " + pk);
                        return false;
                    }


                    //-----------------find the foreign key-----------------
                } else if (UppercaseAttribute.startsWith("FOREIGNKEY")) {

                    // regex names out
                    String fk = attribute.replaceAll("(?i)(foreignkey|references)", "").replace(";", "");
                    fk = fk.replace(" ", "").replaceAll("[()]", " ").strip();

                    //attrib -> tablename attrib
                    String[] fkSpit = fk.split(" ");

                    // check for keyword
                    if (fkSpit.length < 3 || isiIllLegalName(fkSpit[0].toLowerCase())) {
                        System.err.println("ERROR: foreignkey name was a keyword");
                        return false;
                    }

                    VerbosePrint.print(Arrays.toString(fkSpit));
                    TableForeignkeys.add(new ForeignKey(fkSpit[1], fkSpit[2], fkSpit[0]));


                    //-----------------find the attributes-----------------

                } else {
                    //regex names types and Constraints out
                    String[] AttributeSplit = attribute.split(" ");
                    String AttributeName = AttributeSplit[0];
                    String AttributeType = AttributeSplit[1];
                    ArrayList<String> AttributeConstraint = new ArrayList<>();


                    // check for keyword
                    if (isiIllLegalName(AttributeName.toLowerCase())) {
                        return false;
                    }
                    if (!isLegalType(AttributeSplit[1])) {
                        System.err.println("bad type:" + AttributeSplit[1]);
                        return false;
                    }

                    // check for Attribute Constraints
                    if (AttributeSplit.length > 2) {
                        AttributeConstraint.addAll(Arrays.asList(AttributeSplit).subList(2, AttributeSplit.length));
                        // check for keywords allowed
                        AttributeConstraint.removeIf(constraint -> !constraint.equalsIgnoreCase("notnull") && !constraint.equalsIgnoreCase("primarykey"));
                    }
                    VerbosePrint.print("attribute NAME: {" + AttributeName + "} TYPE: {" + AttributeType +
                            "} CONSTRAINTS: " + AttributeConstraint);

                    TableAttributes.add(new Attribute(AttributeName, AttributeType));
                    if (AttributeConstraint.contains("notnull")) {
                        notNullIndexs.add(numberOfNewAttribs);
                    }
                    numberOfNewAttribs++;

                }
            }


            //-------------checking and making new table-----------

            // getiing table
            Catalog cat = (Catalog) Catalog.getCatalog();


            // checking fks are correct
            for (ForeignKey fk : TableForeignkeys) {

                //test ref table exist
                if (!cat.containsTable(fk.getRefTableName())) {
                    System.err.println("Foreign key: " + fk + " references unknown table: " + fk.getRefTableName());
                    return false;
                }

                //test types match in both tables for attributes
                Table tab = (Table) cat.getTable(fk.getRefTableName());
                if (!tab.getAttrByName(fk.getRefAttribute()).getAttributeType()
                        .equals(primaryKey.getAttributeType())) {
                    System.err.println("Foreign key: " + fk + " reference and attribute types mismatch");
                    return false;
                }
            }

            // make new table
            cat.addTable(TableName, TableAttributes, primaryKey);
            ((Table) cat.getTable(TableName)).setForeignKeys(TableForeignkeys);
            // TODO IN table null indexes to table


            return true;

        } catch (Exception e) {

            System.err.println("unknown error in creating new table\n" +
                    "make sure catalog exists before running function");
            return false;
        }
    }

    // check that a type for an atrribute is legal
    private static boolean isLegalType(String TypeName) {
        switch (TypeName) {
            case "Integer":
                return true;
            case "Double":
                return true;
            case "Boolean":
                return true;
            default:
                if (TypeName.startsWith("Char(") || TypeName.startsWith("Varchar(")) {
                    int Lparen = TypeName.indexOf("(");
                    int Rparen = TypeName.indexOf(")");

                    // ()
                    if (Rparen == Lparen + 1 || Rparen == -1) {
                        return false;
                    }
                    //(nums)
                    String num = TypeName.substring(Lparen + 1, Rparen);
                    // all numbers
                    return num.chars().allMatch(Character::isDigit);
                }
        }
        return false;

    }

    // will decide if a name follows rules of
    // 1) not being a key word && 2) starting with a letter and only having alphanumeric
    private static boolean isiIllLegalName(String name) {

        if (KEYWORDS.contains(name)) {
            System.err.println("name: " + name + " is a keyword and cant be used");
            return true;
        }
        if (!(name.chars().allMatch(Character::isLetterOrDigit) && (name.charAt(0) >= 'A' && name.charAt(0) <= 'z'))) {
            System.err.println("name must start with letter and only contain alphanumerics");
            return true;
        }

        return false;
    }

    public static boolean dropTable(String stmt){
        try {

            String TableName;

            //-----------------find the table name key-----------------
            stmt = stmt.substring(11);
            stmt = stmt.replace("\n", "");
            TableName = stmt.substring(0, stmt.indexOf(";")); //TODO: find out if semicolor will always be here
            TableName = TableName.trim();

            Catalog cat = (Catalog) Catalog.getCatalog();
            // Dropping table
            return cat.dropTable(TableName);

        } catch (Exception e) {

            System.err.println("unknown error in removing table\n" +
                    "make sure catalog exists before running function");
            return false;
        }
    }

    public static boolean alterTable(String stmt) {
        return false;
    }





    public static void main(String[] args) {
        DDLParser.parseDDLStatement("""
                creAte taBle A0o(
                        bAz Varchar(10),
                        bar1 Double notnull,
                        primarykey(bar1),
                        foreignkey( bar3 ) REFERENCES bazzle1( baz6 ),
                        FOREIGNKEY(bar4) references bazzle2( baz7 ),
                        foreignkey(cat) references bazzle3(baz8)
                );""");
    }




}
