package parsers;

import catalog.ACatalog;
import catalog.Catalog;
import common.*;
import storagemanager.AStorageManager;
import storagemanager.StorageManager;
import catalog.Catalog;
import common.ITable;
import storagemanager.StorageManager;

import java.util.*;

/*
  Class for DML parser

  This class is responsible for parsing DDL statements

  You will implement the parseDMLStatement and parseDMLQuery functions.
  You can add helper functions as needed, but the must be private and static.

  @author Scott C Johnson (sxjcs@rit.edu)
  @author Aryan Jha (axj2613@rit.edu)

 */
public class DMLParser {

    /**
     * This function will parse and execute DML statements (insert, delete, update, etc)
     * <p>
     * This will be used for parsing DML statement that do not return data
     *
     * @param stmt the statement to parse/execute
     * @return true if successfully parsed/executed; false otherwise
     */
    public static boolean parseDMLStatement(String stmt) {

        if (stmt.toUpperCase().startsWith("DELETE")) {
            return deleteFromTable(stmt);
        }
        if (stmt.toUpperCase().startsWith("INSERT")) {
            return insertToTable(stmt);
        }
        if (stmt.toUpperCase().startsWith("UPDATE")) {
            return updateTable(stmt);
        }
        if (stmt.toUpperCase().startsWith("SELECT")) {
            parseDMLQuery(stmt);
        }
        return false;
    }

    /**
     * This function evaluates the mathematical result of a given 'set' expression of an update statement
     *
     * @param attributeType
     * @param statement1     statement to the left of the operator
     * @param operator       math operator
     * @param statement2     statement to the right of the operator
     * @param attributesList
     * @param row            the row from the table that meets the 'where' condition
     * @return
     */
    private static Object evalSetMath(String attributeType, String statement1, String operator, String statement2,
                                      ArrayList<Attribute> attributesList, ArrayList<Object> row) {
        attributeType = attributeType.toLowerCase();
        switch (attributeType) {
            case "integer":
                int attr1Int = 0;
                boolean attr1IntChanged = false;
                int attr2Int = 0;
                boolean attr2IntChanged = false;
                // check if the statements are placeholder variables
                for (int i = 0; i < attributesList.size(); i++) {
                    if (attributesList.get(i).getAttributeName().equals(statement1)) {
                        attr1Int = (int) row.get(i);
                        attr1IntChanged = true;
                    }
                    if (attributesList.get(i).getAttributeName().equals(statement2)) {
                        attr2Int = (int) row.get(i);
                        attr2IntChanged = true;
                    }
                }
                // if the statements are not placeholder variables, set them as literal int values
                if (!attr1IntChanged) {
                    attr1Int = Integer.parseInt(statement1);
                }
                if (!attr2IntChanged) {
                    attr2Int = Integer.parseInt(statement2);
                }
                // perform math on the statements
                switch (operator) {
                    case "+":
                        return attr1Int + attr2Int;
                    case "-":
                        return attr1Int - attr2Int;
                    case "*":
                        return attr1Int * attr2Int;
                    case "/":
                        return attr1Int / attr2Int;
                }
            case "double":
                double attr1Double = 0;
                boolean attr1DoubleChanged = false;
                double attr2Double = 0;
                boolean attr2DoubleChanged = false;
                // check if the statements are placeholder variables
                for (int i = 0; i < attributesList.size(); i++) {
                    if (attributesList.get(i).getAttributeName().equals(statement1)) {
                        attr1Double = (double) row.get(i);
                        attr1DoubleChanged = true;
                    }
                    if (attributesList.get(i).getAttributeName().equals(statement2)) {
                        attr2Double = (double) row.get(i);
                        attr2DoubleChanged = true;
                    }
                }
                // if the statements are not placeholder variables, set them as literal double values
                if (!attr1DoubleChanged) {
                    attr1Double = Double.parseDouble(statement1);
                }
                if (!attr2DoubleChanged) {
                    attr2Double = Double.parseDouble(statement2);
                }
                // perform math on the statements
                switch (operator) {
                    case "+":
                        return attr1Double + attr2Double;
                    case "-":
                        return attr1Double - attr2Double;
                    case "*":
                        return attr1Double * attr2Double;
                    case "/":
                        return attr1Double / attr2Double;
                }
            default:
                return null;
        }
    }

    /**
     * This function converts an attribute to the given attributeType. The attribute given can be a mathematical expression.
     *
     * @param attributeType
     * @param attribute
     * @param attributesList
     * @param row            the row from the table that meets the 'where' condition
     * @return
     */
    private static Object convertAttributeType(String attributeType, String attribute, ArrayList<Attribute> attributesList,
                                               ArrayList<Object> row) {
        String[] expression = attribute.split(" ");
        attributeType = attributeType.toLowerCase();
        try {
            switch (attributeType) {


                case "integer":
                    if (expression.length == 1) {
                        return Integer.parseInt(attribute);
                    } else {
                        return evalSetMath(attributeType, expression[0], expression[1], expression[2], attributesList, row);
                    }
                case "double":
                    if (expression.length == 1) {
                        return Double.parseDouble(attribute);
                    } else {
                        return evalSetMath(attributeType, expression[0], expression[1], expression[2], attributesList, row);
                    }
                case "boolean":
                    return Boolean.parseBoolean(attribute);
                default:
                    return attribute;
            }
        } catch (Exception e) {
            return null;
        }
    }

    // delete from <tableName> where <condition>
    private static boolean deleteFromTable(String stmt) {

        try {
            stmt = stmt.replace("\"", "");
            // removes redundant spaces and new lines
            stmt = stmt.replace(";", "");
            // tokenizing tokens
            List<String> tokens = Utilities.mkTokensFromStr(stmt);

            // getting the tablet o remove from
            String tableName = tokens.get(2);

            VerbosePrint.print("deleting from table name:" + tableName);

            // cehck table exists
            ITable table = Catalog.getCatalog().getTable(tableName);

            // if no where clause then remove all from table
            boolean removeEverything = tokens.size() == 3;
            if (removeEverything) {
                VerbosePrint.print("removing everyting from table : " + tableName);
                return ((StorageManager) StorageManager.getStorageManager()).deleteRecordWhere(table, "", true);

            }
            //WHERE CLAUSE found
            String Where = String.join(" ", tokens.subList(4, tokens.size())).replace(";", "");
//            System.out.println("where{" + Where + "}");
            // deleteing where is true
            return ((StorageManager) StorageManager.getStorageManager()).deleteRecordWhere(table, Where, false);


        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("error in removing in DML");
            return false;
        }


    }

    // insert into <name> values <tuples>
    private static boolean insertToTable(String stmt) {
        try {
            // removes redundant spaces and new lines
            stmt = Utilities.format(stmt);
            stmt = stmt.replace("( ", "(");
            stmt = stmt.replace(" )", ")");
            stmt = stmt.replace(",", " ");
            stmt = stmt.replace("\"", "");
            stmt = stmt.replace(";", "");
            List<String> tokens = Utilities.mkTokensFromStr(stmt);

            if (tokens.size() == 5 && tokens.get(4).equals("(1)")) {
                int q = 0;
            }

            VerbosePrint.print(tokens);

            String tableName = tokens.get(2);

            // check if table exists; get table
            if (!Catalog.getCatalog().containsTable(tableName)) {
                System.err.println("The catalog does not contain the table: " + tableName);
                return false;
            }

            VerbosePrint.print("inserting to table: " + tableName);

            ITable table = Catalog.getCatalog().getTable(tableName);
            ArrayList<Attribute> attributes = table.getAttributes();

            int numberOfInserts = 0;
            if (tokens.get(3).equalsIgnoreCase("values")) {
                for (int i = 4; i < tokens.size(); i++) {
                    ArrayList<Object> record = new ArrayList<>();
                    if (!tokens.get(i).startsWith("(")) {
                        System.err.println("The tuples in the insert statement are not in the correct format; " +
                                "misplaced/missing opening parenthesis");
                        return false;
                    }

                    if (attributes.size() == 1) {
                        record.add(convertAttributeType(attributes.get(i - (4 + (attributes.size() * numberOfInserts)))
                                .getAttributeType(), tokens.get(i).substring(1, tokens.get(i).length() - 1), null, null));
                    } else {
                        record.add(convertAttributeType(attributes.get(i - (4 + (attributes.size() * numberOfInserts)))
                                .getAttributeType(), tokens.get(i).substring(1), null, null));
                        i++;

//                        System.out.println(record+"<-------n"+ tokens);

                        while (!tokens.get(i).endsWith(")")) {
                            record.add(convertAttributeType(attributes.get(i - (4 + (attributes.size() * numberOfInserts)))
                                    .getAttributeType(), tokens.get(i), null, null));
                            i++;
                        }

                        if (!tokens.get(i).strip().equals(")")) {
                            record.add(convertAttributeType(attributes.get(i - (4 + (attributes.size() * numberOfInserts)))
                                    .getAttributeType(), tokens.get(i).substring(0, tokens.get(i).length() - 1), null, null));
                        }
                    }

//                    System.out.println(record);

                    if (record.size() == attributes.size()) {
                        boolean insertSuccess = StorageManager.getStorageManager().insertRecord(table, record);
//                        System.out.println("insert success: " + insertSuccess);
                        if (!insertSuccess) {
                            return false;
                        }
                    } else {
                        System.err.println("The tuples in the insert statement are not in the correct format; " +
                                "incorrect number of attributes");
                        return false;

                    }
                    numberOfInserts++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("error in inserting using DML");
            return false;

        }
        return true;////////////
    }

    // update <name> set <column_1> = value where <condition>;
    private static boolean updateTable(String stmt) {
        try {
            stmt = stmt.replace("\"", "");
            // removes redundant spaces and new lines
            stmt = stmt.replace(";", "");
            List<String> tokens = Utilities.mkTokensFromStr(stmt);
//            System.out.println(tokens);

            String tableName = tokens.get(1);

            // check if table exists; get table
            if (!Catalog.getCatalog().containsTable(tableName)) {
                System.err.println("The catalog does not contain the table: " + tableName);
                return false;
            }


            // does not have attrib
            boolean hasAttrib = false;
            for (Attribute attributee : Catalog.getCatalog().getTable(tableName).getAttributes()) {
                if (attributee.getAttributeName().equals(tokens.get(3))) {
                    hasAttrib = true;
                    break;
                }
            }
            if (!hasAttrib) {
                System.err.println("The table " + tableName + " does not contain the attribute: " + tokens.get(3));
                return false;
            }


//            System.out.println("updating the table: " + tableName);

            ITable table = Catalog.getCatalog().getTable(tableName);

            ArrayList<ArrayList<Object>> records = StorageManager.getStorageManager().getRecords(table);
            ArrayList<Attribute> attributes = table.getAttributes();
//            ArrayList<Attribute> attributes = ((Table)table).indicesOfNotNullAttributes;

            boolean updateSuccess = false;

            if (tokens.get(6).equalsIgnoreCase("where")) {
                WhereParser wp = new WhereParser();
                for (ArrayList<Object> row : records) {
                    if (wp.whereIsTrue(stmt, row, attributes)) {
                        if (tokens.get(2).equalsIgnoreCase("set")) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            for (int i = 0; i < attributes.size(); i++) {
                                if (attributes.get(i).getAttributeName().equals(tokens.get(3))) {
                                    newRow.add(convertAttributeType(attributes.get(i).getAttributeType(), tokens.get(5), attributes, row));
                                } else {
                                    newRow.add(row.get(i));
                                }
                            }
                            updateSuccess = StorageManager.getStorageManager().updateRecord(table, row, newRow);
//                            System.out.println("update success:" + updateSuccess);
                            if (!updateSuccess) {
                                StorageManager.getStorageManager().insertRecord(table, row);
                            }
                            return updateSuccess;
                        }
                    }
                }
            } else if (tokens.size() > 10 && tokens.get(8).equalsIgnoreCase("where")) {
                WhereParser wp = new WhereParser();
                for (ArrayList<Object> row : records) {
                    if (wp.whereIsTrue(stmt, row, attributes)) {
                        if (tokens.get(2).equalsIgnoreCase("set")) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            for (int i = 0; i < attributes.size(); i++) {
                                if (attributes.get(i).getAttributeName().equals(tokens.get(3))) {
                                    newRow.add(convertAttributeType(attributes.get(i).getAttributeType(), tokens.get(5)
                                            + " " + tokens.get(6) + " " + tokens.get(7), attributes, row));
                                } else {
                                    newRow.add(row.get(i));
                                }
                            }
                            updateSuccess = StorageManager.getStorageManager().updateRecord(table, row, newRow);
                            if (!updateSuccess) {
                                StorageManager.getStorageManager().updateRecord(table, newRow, row);
                            }
//                            System.out.println("update success:" + updateSuccess);
                            return updateSuccess;
                        }
                    }
                }
            }

            return updateSuccess;

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("error in updating using DML");
            return false;
        }
    }

    public static ITable selectFrom(List<String> tables) {

        HashSet<ArrayList<Object>> seen = new HashSet<>();
        Set<String> set = new HashSet<String>(tables);
        if (set.size() < tables.size()) {
            System.err.println("Invalid select statement: duplicate table names in 'from'");
            return null;
        }

        if (tables.size() == 1) {
            if (!Catalog.getCatalog().containsTable(tables.get(0))) {
                System.err.println("Invalid select statement: table " + tables.get(0) + " in 'from' does not exist");
                return null;
            }
            return new Table(Catalog.getCatalog().getTable(tables.get(0)));  // new Table makes a copy of that table
        } else {
            if (!Catalog.getCatalog().containsTable(tables.get(0))) {
                System.err.println("Invalid select statement: table " + tables.get(0) + " in 'from' does not exist");
                return null;
            }

            ITable table0 = Catalog.getCatalog().getTable(tables.get(0));
            ArrayList<Attribute> attributes0 = table0.getAttributes();
            ArrayList<Attribute> newAttributes0 = new ArrayList<>();
            newAttributes0.add(new Attribute("primaryKey", "Integer"));
            Attribute pk0 = newAttributes0.get(0);
            for (Attribute attr : attributes0) {
                newAttributes0.add(new Attribute(tables.get(0) + "." + attr.getAttributeName(),
                        attr.attributeType()));
            }

            ITable cartProd = new Table("cartesianProduct", newAttributes0, pk0);
            int serial = 1;
            for (ArrayList<Object> row : StorageManager.getStorageManager().getRecords(table0)) {
                ArrayList<Object> record = new ArrayList<>();
                record.add(serial);

                record.addAll(row);


                StorageManager.getStorageManager().insertRecord(cartProd, record);
                serial++;

            }

            for (int i = 1; i < tables.size(); i++) {
                if (!Catalog.getCatalog().containsTable(tables.get(i))) {
                    System.err.println("Invalid select statement: table" + tables.get(i) + "in 'from' does not exist");
                    return null;
                }
                ITable table = Catalog.getCatalog().getTable(tables.get(i));
                ArrayList<Attribute> attributes = table.getAttributes();
                ArrayList<Attribute> newAttributes = new ArrayList<>(cartProd.getAttributes());
                Attribute pk = newAttributes.get(0);
                for (Attribute attr : attributes) {
                    newAttributes.add(new Attribute(tables.get(i) + "." + attr.getAttributeName(),
                            attr.attributeType()));
                }
                ITable cartProdTemp = new Table("cartesianProduct", newAttributes, pk);

                serial = 1;
                for (ArrayList<Object> cpRow : StorageManager.getStorageManager().getRecords(cartProd)) {
                    for (ArrayList<Object> row : StorageManager.getStorageManager().getRecords(table)) {
                        ArrayList<Object> record = new ArrayList<>();
                        record.add(serial);
                        record.addAll(cpRow.subList(1, cpRow.size()));
                        record.addAll(row);
                        StorageManager.getStorageManager().insertRecord(cartProdTemp, record);
                        serial++;
                    }
                }
                cartProd = cartProdTemp;
            }
            return cartProd;
        }
    }


    /**
     * This function will parse and execute DML statements (select)
     * <p>
     * This will be used for parsing DML statement that return data
     *
     * @param query the query to parse/execute
     * @return the data resulting from the query; null upon error.
     * Note: No data and error are two different cases.
     */
    public static ResultSet parseDMLQuery(String query) {

        //ensure formated correctly
        query = Utilities.format(query);
        String LowerQueryStmt = query.toLowerCase().replace(",", " ").replace(";", "");
        String OriginalQueryStmt = query.replace(",", " ").replace(";", "");
        OriginalQueryStmt = Utilities.format(OriginalQueryStmt);

        List<String> StmtTokens = Utilities.mkTokensFromStr(LowerQueryStmt);
        List<String> StmtTokensOriginal = Utilities.mkTokensFromStr(OriginalQueryStmt);


        // ----------------- ----------------- FROM | make cartesian prod table -----------------

        int fromStart = StmtTokens.indexOf("from")+1;
        List<String> tables;
        if (fromStart == 0) {
            System.err.println("Invalid select statement: missing 'from'");
            return null;
        } else {
            int fromEnd = StmtTokens.indexOf("where");  // jank way to detect end of statement
            if (fromEnd == -1){
                fromEnd = StmtTokens.indexOf("orderby");
            }
            if (fromEnd == -1){
                fromEnd = StmtTokens.size();  // the last token
            }
            if (fromEnd == -1) {
                System.err.println("Invalid select statement: missing \";\"");
                return null;
            } else {
                tables = StmtTokensOriginal.subList(fromStart, fromEnd);
            }
        }

//        System.out.println(tables);

        if (tables.size() == 0) {
            System.err.println("Invalid select statement: missing <tables> in 'from'");
            return null;
        }
        tables.forEach(t -> t = t.replace(",", ""));
//        tables.forEach(t -> t = t.replace(";", ""));

        ITable cartProd = selectFrom(tables);
        if (cartProd == null) {
            System.err.println("Something went wrong in getting the cartesian product of tables in 'from'");
            return null;
        }



        Table table = (Table) cartProd;

//        StorageManager.getStorageManager().dropAttributeValue(cartProd,0);
//        ArrayList<ArrayList<Object>> v = StorageManager.getStorageManager().getRecords(table);
//        ResultSet r = Utilities.ResultSetFromTable(table.getAttributes(), v);
//        Utilities.prettyPrintResultSet(r,false,10);
//        System.exit(1);
//        System.out.println(table.tableToString());

        // -----------------WHERE | do where on cartesian prod table-----------------------

        // ----------------- will remove all unqualified rows -----------------------------


        int whereIdx = LowerQueryStmt.indexOf("where");
        int fromIdx = LowerQueryStmt.indexOf("from");
        int semiIdx = LowerQueryStmt.indexOf(";");
        int orderbyIdx = LowerQueryStmt.indexOf("orderby");

        if (whereIdx > 0) {
            int stopIdx = semiIdx;
            if (semiIdx != -1 || orderbyIdx != -1) {
                if (semiIdx == -1) {
                    stopIdx = orderbyIdx;
                } else {
                    stopIdx = Math.min(orderbyIdx, semiIdx);
                }
                String WhereStmt = query.substring(whereIdx, stopIdx);

                // parse table unqualified rows
                ((StorageManager) StorageManager.getStorageManager()).keepWhere(table, WhereStmt, false);
            } else {
                System.err.println("error in stmt");
                return null;
            }

        }


        //  ----------------- ----------------- SELECT | get only columns we asked for -----------------

        // get the string containing only the attributes we want (doesn't include the word "select")
        String wantedAttrString = query.substring(LowerQueryStmt.indexOf("select") + "select".length() + 1, fromIdx);
        String[] wantedAttrs = wantedAttrString.split(" |, ");  //lazy regex move, will split on space or ,space
        System.out.println("Debug: " + Arrays.deepToString(wantedAttrs));  //TODO remove debug

        // check for star

        if (wantedAttrs.length > 1 && wantedAttrString.contains("*")) { // there's a star, but it's not the only attribute
            System.err.println("Improper use of \"*\".  Cannot combine \"*\" with other attributes.");
        }
        if (!(wantedAttrs.length == 1 && wantedAttrs[0].equals("*"))) { //if there's a star, leave the table in tact
            //make a list out of our array, then a hashset out of that list to send to Select function
            if (!Utilities.Select(table, new HashSet<>(Arrays.asList(wantedAttrs)))) {
                System.out.println("Error with selected attributes");
                return null;
            }
        }

        //
        //  ----------------- ----------------- ORDER-BY | SORT rows ----------------- -----------------


        ArrayList<ArrayList<Object>> records = (StorageManager.getStorageManager()).getRecords(table);


        if (orderbyIdx != -1) {
            String[] sortByAttributeName = query.substring(orderbyIdx).replace(";", "").split(" ");
            if (sortByAttributeName.length < 2) {
                System.err.println("OrderBy: column name to order by is missing");
                return null;
            }
            records = Utilities.SortBy(table, records, sortByAttributeName[1], false);
            if (records == null){
                return null;
            }



        }

        HashSet<ArrayList<Object>>seen = new HashSet<>();
        ArrayList<ArrayList<Object>> finRecs = new ArrayList<>();
        for(ArrayList<Object> r:records){
            if(!seen.contains(r)){
                finRecs.add(r);
            }
            seen.add(r);

        }
        ResultSet rs = new ResultSet(table.getAttributes(), finRecs);

        // RETURN RESULT-SET
        return rs;
    }


    public static void main(String[] args) {
        ACatalog catalog = Catalog.createCatalog("/Users/aryanjha/Documents/CSCI 421/DBMS/DB", 4048, 10);
        AStorageManager sm = AStorageManager.createStorageManager();

        ArrayList<Attribute> attributes = new ArrayList<>();

        attributes.add(new Attribute("ID", "Integer"));
        attributes.add(new Attribute("Name", "VarChar(20)"));
        attributes.add(new Attribute("Goals", "Integer"));

        Attribute pk = attributes.get(0);

        ITable table1 = catalog.addTable("goalScorers", attributes, pk);
        ITable table2 = catalog.addTable("soalGcorers", attributes, pk);

        // test insert
        DMLParser.parseDMLStatement("insert into goalScorers \n values \n (1, \"Karim Benzema\", 7)," +
                "(2, \"Kylian Mbappe\", 1)");
        DMLParser.parseDMLStatement("insert into goalScorers \n values \n (1, \"Thomas Muller\", 1)");
        // test update
        System.out.println("Before update call: " + sm.getRecords(table1));
        DMLParser.parseDMLStatement("update goalScorers \n set Goals = ID + 2 \n where ID = 1");
        System.out.println("After update call: " + sm.getRecords(table1));

        DMLParser.parseDMLStatement("select id from goalScorers, soalGcorers where goals > 2;");
    }
}