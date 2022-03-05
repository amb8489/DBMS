package parsers;

import catalog.Catalog;
import common.Attribute;
import common.ForeignKey;
import common.Table;
import common.VerbosePrint;

import java.nio.ByteBuffer;
import java.util.*;
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

    // list of key words not allowed for use for table/ column names

    //TODO add types
    // possible move
    // split(,#)
    private static Set<String> KEYWORDS = Stream.of(
            "create", "table", "drop", "insert", "into", "delete", "from", "where", "update",
            "notnull", "primarykey", "foreignkey", "references", "add", "default", "set",
            "values", "null").collect(Collectors.toCollection(HashSet::new));



    /**
     * This function will parse and execute DDL statements (create table, create index, etc)
     *
     * @param stmt the statement to parse
     * @return true if successfully parsed/executed; false otherwise
     */

    public static boolean parseDDLStatement(String stmt) {

        stmt = StringFormatter.format(stmt);
        System.out.println(stmt);

        if (stmt.toLowerCase().startsWith("create table")) {
            return CreateTable(stmt);
        }else if (stmt.toLowerCase().startsWith("drop table")) {
            return dropTable(stmt);
        }else if (stmt.toLowerCase().startsWith("alter table")) {
            return alterTable(stmt);
        }

        return false;
    }
    // will create and add table to the DB given a Create table statement
    // TODO check for dups on all p attributes and pk check that table doesn't already exist

    // check attributes exits when adding
    private static boolean CreateTable(String stmt) {

        try {
            stmt =stmt.replace(";","");

            // vars for the new table
            ArrayList<Attribute> TableAttributes = new ArrayList<>();
            Attribute primaryKey = null;
            ArrayList<ForeignKey> TableForeignkeys = new ArrayList<>();
            Set<Integer> notNullIndexs = new HashSet<>();
            String TableName;


            //-----------------finding the table name key-----------------
            stmt = stmt.substring(13);
            stmt = stmt.replace("\n", "");

            // parsing the table name
            TableName = stmt.substring(0, stmt.indexOf("("));

            // check that name is not keyword
            if (isiIllLegalName(TableName.toLowerCase())) {
                return false;
            }
            VerbosePrint.print("table name" + TableName);


            // removing the name from the string
            stmt = stmt.substring(TableName.length() + 1);


            // parsing for the rest of the table attributes, pk and fks
            boolean tableHasPk = false;
            int numberOfNewAttribs = 0;

            // splitting on comma
            // for each token
            for (String attrib : stmt.split(",")) {

                String attribute = attrib.strip();
                String UppercaseAttribute = attribute.toUpperCase();


                //-----------------find the PRIMARY key-----------------
                if (UppercaseAttribute.startsWith("PRIMARYKEY")) {


                    // regex pk name out
                    String pk = attribute.substring(11).replace(")", "").strip();

                    // check for more than 1 pk
                    if (tableHasPk) {

                        // if pk is not just being redundantly redeclare
                        if (!primaryKey.getAttributeName().equals(pk)) {
                            System.err.println("creating table " + TableName + " cant have more then 1 primary key");
                            return false;
                        }

                        // the pk was already defined earlier
                        continue;
                    }

                    // table does not have a p yet


                    // check pk name is not keyword
                    if (isiIllLegalName(pk.toLowerCase())) {
                        return false;
                    }
                    VerbosePrint.print("primarykey: {" + pk + "}");

                    // finding the index of the pk in the attributes
                    int pkidx = 0;
                    for (Attribute att : TableAttributes) {
                        if (att.getAttributeName().equals(pk)) {
                            // setting the pk
                            primaryKey = att;
                            tableHasPk = true;
                            // pk are always notnull
                            notNullIndexs.add(pkidx);
                            break;
                        }
                        pkidx++;
                    }

                    // if setting pk to something that's not in the table
                    if (!tableHasPk) {
                        System.err.println("table does not have attribute " + pk);
                        return false;
                    }


                    //-----------------find the foreign key-----------------
                } else if (UppercaseAttribute.startsWith("FOREIGNKEY")) {

                    // regex names out of string and removing ;
                    String fk = attribute.replaceAll("(?i)(foreignkey|references)", "").replace(";", "");

                    // replacing ( ) with spaces to be able to split on space
                    fk = fk.replace(" ", "").replaceAll("[()]", " ").strip();

                    //splittiing [attrib, ref-table-name, attrib in ref table]
                    String[] fkSpit = fk.split(" ");

                    // check for keyword is name
                    if (fkSpit.length < 3 || isiIllLegalName(fkSpit[0].toLowerCase())) {
                        System.err.println("ERROR: foreignkey name was a keyword");
                        return false;
                    }

                    VerbosePrint.print("fk: "+Arrays.toString(fkSpit));

                    // mk new fk
                    TableForeignkeys.add(new ForeignKey(fkSpit[1], fkSpit[2], fkSpit[0]));


                    //-----------------find the attributes-----------------

                } else {
                    //regex names types and Constraints out
                    String[] AttributeSplit = attribute.split(" ");
                    String AttributeName = AttributeSplit[0];
                    String AttributeType = AttributeSplit[1];
                    ArrayList<String> AttributeConstraint = new ArrayList<>();


                    // check for keyword in name
                    if (isiIllLegalName(AttributeName.toLowerCase())) {
                        return false;
                    }
                    // check for bad type of attribute
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


                    Attribute newAttribute= new Attribute(AttributeName, AttributeType);
                    TableAttributes.add(newAttribute);

                    // check constraints for attribute
                    for (String constraint : AttributeConstraint) {

                        if (constraint.equalsIgnoreCase("notnull") ) {
                            notNullIndexs.add(numberOfNewAttribs);
                        }else
                        if (constraint.equalsIgnoreCase("primarykey") ){
                            primaryKey = newAttribute;
                            tableHasPk = true;
                            notNullIndexs.add(numberOfNewAttribs);
                        }
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



                boolean hasAttrib = false;
                for (Attribute a : tab.getAttributes()) {
                    if (a.getAttributeName().equals(fk.getRefAttribute())) {
                        hasAttrib=true;
                        break;
                    }
                }
                if (!hasAttrib){
                    System.err.println("Attribute "+fk.getRefAttribute() +" does not does not exist in table "+ fk.getRefTableName());
                    return false;
                }




                if (!tab.getAttrByName(fk.getRefAttribute()).getAttributeType().equals(primaryKey.getAttributeType())) {
                    System.err.println("Foreign key: " + fk + " reference and attribute types mismatch");
                    return false;
                }
            }
            // make new table
            cat.addTable(TableName, TableAttributes, primaryKey);
            ((Table) cat.getTable(TableName)).setForeignKeys(TableForeignkeys);
            ((Table) cat.getTable(TableName)).setNotNullIdxs(notNullIndexs);

            return true;

        } catch (Exception e) {

            System.err.println("unknown error in creating new table\n");
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
                    // all numbers in str
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
            TableName = stmt.substring(0, stmt.indexOf(";")-1); //TODO: find out if semicolor will always be here
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
        /**
         * alter table <name> drop <a_name>;
         * alter table <name> add <a_name> <a_type>;
         * alter table <name> add <a_name> <a_type> default <value>;
         */
        String TableName;
        String command;
        String attribute;
        String type;
        String[] stmtTokens;
        String defaultValue;

        //-----------------find the table name key-----------------

        stmt = stmt.substring(12); //<name> command <a_name> <a_type> default <value>
        stmt = stmt.replace("\n", "");
        stmtTokens= stmt.split(" ;");  // ??
        TableName = stmtTokens[0]; //<name>
        command = stmtTokens[1].toLowerCase();
        attribute = stmtTokens[2];
        if (command.equals("add")){
            type = stmtTokens[3];
            if (stmtTokens.length > 4){
                defaultValue = stmtTokens[4];
                //TODO: add the attribute
            }
            else{
                //TODO: add the attribute
            }
        } else if (command.equals("drop")){
            //TODO: no more parsing, just drop the thing
        }
        else{
            //TODO: figure out our error handling across application
        }





        return false;
    }

    public static void main(String[] args) {


        DDLParser.parseDDLStatement("""
                create table foo(
                        baz Integer,
                        bar Double notnull,
                        primarykey( bar ),
                        foreignkey( bar ) references bazzle( baz )   )   ;""");
    }




}
