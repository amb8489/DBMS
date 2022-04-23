package parsers;

import catalog.Catalog;
import common.*;
import indexing.BPlusTree;
import storagemanager.StorageManager;

import java.util.*;

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

    public static boolean parseDDLStatement(String stmt) {

        stmt = Utilities.format(stmt);

        if (stmt.toLowerCase().startsWith("create table")) {
            return CreateTable(stmt);
        } else if (stmt.toLowerCase().startsWith("drop table")) {
            return dropTable(stmt);
        } else if (stmt.toLowerCase().startsWith("alter table")) {
            return alterTable(stmt);
        } else if (stmt.toLowerCase().startsWith("create index")) {
            return createIndex(stmt);
        } else {
            return false;
        }
    }

    private static boolean createIndex(String stmt) {
        try {

            stmt = stmt.replace("(", " ");
            stmt = stmt.replace(")", " ");
            stmt = stmt.replace(";", "");

            List<String> tokens = Utilities.mkTokensFromStr(stmt);

            String tableName = tokens.get(4);

            // check if table exists; get table
            if (!Catalog.getCatalog().containsTable(tableName)) {
                System.err.println("The catalog does not contain the table: " + tableName);
                return false;
            }

            Table table = (Table) Catalog.getCatalog().getTable(tableName);

            int attrIdx = -1;
            ArrayList<Attribute> attributes = table.getAttributes();
            for (int i = 0; i < attributes.size(); i++) {
                if (attributes.get(i).getAttributeName().equals(tokens.get(5))) {
                    attrIdx = i;
                    break;
                }
            }
            if (attrIdx == -1) {
                System.err.println("The table " + tableName + " does not contain the attribute: " + tokens.get(5));
                return false;
            }

            BPlusTree bTree = BPlusTree.TreeFromTableAttribute(table, attrIdx);

            // TODO: change this to a set call
            table.IndexedAttributes.put(tokens.get(5), bTree);

            return true;

        } catch (Exception e) {
            System.err.println("DDlParser: Error in creating a index");
            return false;
        }
    }

    // will create and add table to the DB given a Create table statement
    // TODO check for dups on all p attributes and pk check that table doesn't already exist
    // TODO check attributes exits when adding
    private static boolean CreateTable(String stmt) {

        try {
            stmt = stmt.replace(";", "");

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
            if (Utilities.isiIllLegalName(TableName.toLowerCase())) {
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
                    if (Utilities.isiIllLegalName(pk.toLowerCase())) {
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
                    if (fkSpit.length < 3 || Utilities.isiIllLegalName(fkSpit[0].toLowerCase())) {
                        System.err.println("ERROR: foreignkey name was a keyword");
                        return false;
                    }

                    VerbosePrint.print("fk: " + Arrays.toString(fkSpit));

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
                    if (Utilities.isiIllLegalName(AttributeName.toLowerCase())) {
                        return false;
                    }
                    // check for bad type of attribute
                    if (Utilities.isNotLegalType(AttributeSplit[1])) {
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


                    Attribute newAttribute = new Attribute(AttributeName, AttributeType);
                    TableAttributes.add(newAttribute);

                    // check constraints for attribute
                    for (String constraint : AttributeConstraint) {

                        if (constraint.equalsIgnoreCase("notnull")) {
                            notNullIndexs.add(numberOfNewAttribs);
                        } else if (constraint.equalsIgnoreCase("primarykey")) {
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
                        hasAttrib = true;
                        break;
                    }
                }
                if (!hasAttrib) {
                    System.err.println("Attribute " + fk.getRefAttribute() + " does not does not exist in table " + fk.getRefTableName());
                    return false;
                }


                if (!tab.getAttrByName(fk.getRefAttribute()).getAttributeType().equals(primaryKey.getAttributeType())) {
                    System.err.println("Foreign key: " + fk + " reference and attribute types mismatch");
                    return false;
                }
            }
            // make new table
            cat.addTable(TableName, TableAttributes, primaryKey);

            Table newTab = ((Table) cat.getTable(TableName));
            for (ForeignKey fk : TableForeignkeys) {
                if (!newTab.addForeignKey(fk)) {
                    return false;
                }
            }
            ((Table) cat.getTable(TableName)).setNotNullIdxs(notNullIndexs);

            return true;

        } catch (Exception e) {

            System.err.println("unknown error in creating new table\n");
            return false;
        }
    }

    // will drop a table from db
    public static boolean dropTable(String stmt) {
        try {

            String TableName;

            //-----------------find the table name key-----------------
            stmt = stmt.substring(11);
            stmt = stmt.replace("\n", "");

            TableName = stmt.replace(";","");
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

        // will guarantee no funky spacing will mess up the parsing
        stmt = stmt.replace(";", "");
        stmt = Utilities.format(stmt);


        /**
         * alter table <name> drop <a_name>;
         * alter table <name> add <a_name> <a_type>;
         * alter table <name> add <a_name> <a_type> default <value>;
         */
        String TableName;
        String command;
        String attributeName;
        String type;
        String[] stmtTokens;
        String defaultValue = null;

        //-----------------find the table name key-----------------


        // removing "alter table " from front of the string
        stmt = stmt.substring(12);

        // tokenizing into [tableName, command, attrib_name, attrib_type, default ,value]
        // first three should  always  exist
        stmtTokens = stmt.split(" ");  // ??

        // check for first three should  always  exist
        if (stmtTokens.length < 3) {
            System.err.println("bad format in alter table: " + stmt);
            return false;
        }


        // getting table name
        TableName = stmtTokens[0];

        // check that table exist
        if (!Catalog.getCatalog().containsTable(TableName)) {
            System.err.println("table: " + TableName + " does not exist");
            return false;
        }

        // getting the command can be add/drop
        command = stmtTokens[1].toLowerCase();

        if (!command.equals("add") && !command.equals("drop")) {
            System.err.println(command + " is not a command for alter table");
            return false;
        }


        attributeName = stmtTokens[2];


        if (command.equals("add")) {
            // if were adding we need at least 4 tokens
            if (stmtTokens.length < 4) {
                System.err.println("bad format in alter table missing arguments in statement: " + stmt);
                return false;
            }
            // check new name is legal
            if (Utilities.isiIllLegalName(attributeName.toLowerCase())) {
                System.err.println("Not a legal name: " + attributeName);
                return false;
            }

            // check that attribute doesnt alreaady exist in table


            for (Attribute attribute : Catalog.getCatalog().getTable(TableName).getAttributes()) {
                if (attribute.getAttributeName().equals(attributeName)) {
                    System.err.println("attribute " + attributeName + " already exists in table: " + TableName);
                    return false;
                }
            }

            // getting attribute type
            type = stmtTokens[3];
            // check that type is a legal type
            if (Utilities.isNotLegalType(type)) {
                System.err.println("type " + type + " is not a legal type");
                return false;
            }


            // we have a default val

            if (stmtTokens.length > 4) {
                //check that default val type matches the type of the attribute
                defaultValue = stmtTokens[5];

                String defaultValueType = Utilities.whatType(defaultValue);

                if (defaultValueType == null) {
                    System.err.println("type " + type + " is not a legal type");
                    return false;
                } else {

                    // if our default is a string let check that its legal for the type of string
                    if (defaultValueType.equals("String")) {



                        // was the type given a string as well
                        if (type.toLowerCase().startsWith("char(") || type.toLowerCase().startsWith("varchar(")) {


                            // lets get how big the string can be and if its too big or small

                            if(Utilities.isStringTooLong(type,defaultValue)) {

                                System.err.println("string: "+defaultValue +" too long for type: "+defaultValueType);
                                return false;
                            }

                        } else {
                            System.err.println("types for new attribute and default value dont match: " + type + " with " + defaultValueType + " | default val:" + defaultValue);
                            return false;
                        }
                    } else {
                        if (!defaultValueType.equalsIgnoreCase(type)) {
                            System.err.println("types for new attribute and default value dont match: " + type + " with " + defaultValueType + " | default val:" + defaultValue);
                            return false;
                        }
                    }
                }
            }
            //add atrribute

            if (Catalog.getCatalog().getTable(TableName).addAttribute(attributeName, type)) {
                // add default val
                StorageManager.getStorageManager().addAttributeValue(Catalog.getCatalog().getTable(TableName), defaultValue);
                return true;
            }
            return false;


        } else {
            // dropping attribute if it exist in the table

            int attribIdx = 0;
            ITable table = Catalog.getCatalog().getTable(TableName);

            for (Attribute attribute : table.getAttributes()) {
                if (attribute.getAttributeName().equals(attributeName)) {

                    return StorageManager.getStorageManager().dropAttributeValue(table, attribIdx);


                }
                attribIdx++;
            }
            // attribute could not be found in the table
            System.err.println("attribute " + attributeName + " does not exists in table: " + TableName);
            return false;
        }

    }

    public static void main(String[] args) {


//        DDLParser.parseDDLStatement("""
//                create table foo(
//                        baz Integer,
//                        bar Double notnull,
//                        primarykey( bar ),
//                        foreignkey( bar ) references bazzle( baz )   )   ;""");
//    }


        DDLParser.parseDDLStatement("""
                alter    
                                
                table   
                bigfooman99          
                                
                                
                                
                add fish Char(8);
                """);
    }
}

//alter table bigfooman99 add <a_name> <a_type>;
//alter table <name> add <a_name> <a_type> default <value>;
