package common;

import catalog.ACatalog;
import catalog.Catalog;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import filesystem.FileSystem;
import indexing.BPlusTree;
import storagemanager.StorageManager;

/*
  Implementation of the ITable interface.  The interface
  @author Kyle Ferguson (krf6081@rit.edu)
  @author Aaron Berghash (amb8489@rit.edu)

 */

public class Table implements ITable {

    public static int numTables = 0; // tracks how many tables.txt have been created; used to establish table ID
    private String TableName;
    private int ID;
    private Attribute PrimaryKey;
    private ArrayList<Attribute> Attributes;

    public HashSet<Integer> indicesOfNotNullAttributes = new HashSet<>();

    private int pkeyIdx = -1;
    private ArrayList<ForeignKey> ForeignKeys = new ArrayList<>();
    private ArrayList<Integer> PagesThatBelongToMe = new ArrayList<>();

    public HashMap<String, Integer> AttribIdxs = new HashMap<>();  // maps attr name to attr index

    //DONT forget to add and delere as we add and delete attriues


    // Attribute name , tree
    public HashMap<String, BPlusTree> IndexedAttributes = new HashMap<>();


    public Table(String tableName, ArrayList<Attribute> Attributes, Attribute PrimaryKey) {
        ID = numTables;
        numTables++;
        this.Attributes = Attributes;
        this.TableName = tableName;
        this.PrimaryKey = PrimaryKey;


        // mapping the attribute name to the idx of that attribute  needed for later
        for (int i = 0; i < Attributes.size(); i++) {

            String name = Attributes.get(i).getAttributeName();

            // . for table specifier in cartesion table prod case
            if (name.contains(".")) {
                String[] splitName = name.split("\\.");
                String attributeName = splitName[1];
                AttribIdxs.put(attributeName, i);
            }
            AttribIdxs.put(name, i);
        }

        Page firstPageForTable = new Page(this);
        firstPageForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), this);

        // make pk tree
        IndexedAttributes.put(this.PrimaryKey.getAttributeName(),
                BPlusTree.TreeFromTableAttribute(this, this.Attributes.indexOf(this.PrimaryKey)));
    }

    // clone table does not add to table count though

    public Table(ITable table) {

        ID = table.getTableId();
        this.Attributes = new ArrayList<>(table.getAttributes());
        this.TableName = table.getTableName();//+"__CLONE__";
        this.PrimaryKey = table.getPrimaryKey();
        this.AttribIdxs = ((Table) table).AttribIdxs;
        this.indicesOfNotNullAttributes = new HashSet<>(((Table) table).indicesOfNotNullAttributes);
        this.pkeyIdx = ((Table) table).pkeyIdx;
        this.ForeignKeys = new ArrayList<>(((Table) table).ForeignKeys);
        this.PagesThatBelongToMe = new ArrayList<>(((Table) table).getPagesThatBelongToMe());
        this.IndexedAttributes = ((Table) table).IndexedAttributes;

    }


    // gets the index of the pk attribute in Table attributes
    public int pkIdx() {

        // first time finding the index of the primany key
        if (pkeyIdx < 0) {
            int idx = 0;
            for (Attribute Attrib : Attributes) {
                if (Attrib.equals(this.PrimaryKey)) {
                    this.pkeyIdx = idx;
                    return idx;
                }
                idx++;
            }
        }
        // already been saved
        return this.pkeyIdx;
    }

    public Table(String tableName, ArrayList<Attribute> tableAttributes, Attribute pk, ArrayList<Integer> belongToMe) {

        ID = numTables;
        this.Attributes = tableAttributes;
        this.TableName = tableName;
        this.PrimaryKey = pk;
        this.PagesThatBelongToMe = belongToMe;

        // mapping the attribute name to the idx of that attribute  needed for later
        for (int i = 0; i < Attributes.size(); i++) {

            String name = Attributes.get(i).getAttributeName();

            // . for table specifier in cartesion table prod case
            if (name.contains(".")) {
                String[] splitName = name.split("\\.");
                String attributeName = splitName[1];
                AttribIdxs.put(attributeName, i);
            }
            AttribIdxs.put(name, i);
        }
    }


    public ArrayList<Integer> getPagesThatBelongToMe() {
        return PagesThatBelongToMe;
    }

    @Override
    public String getTableName() {
        return this.TableName;
    }

    @Override
    public void setTableName(String name) {
        this.TableName = name;
    }

    @Override
    public int getTableId() {
        return this.ID;
    }

    @Override
    public ArrayList<Attribute> getAttributes() {
        return Attributes;
    }

    @Override
    public Attribute getAttrByName(String name) {

        for (Attribute attribute : Attributes) {
            if (attribute.attributeName().equals(name)) {
                return attribute;
            }
        }
        return null;
    }

    @Override
    public Attribute getPrimaryKey() {
        return this.PrimaryKey;
    }

    @Override
    public ArrayList<ForeignKey> getForeignKeys() {
        return this.ForeignKeys;
    }

    @Override
    public boolean addAttribute(String name, String type) {
        // test if attribute already exists
        for (Attribute attribute : Attributes) {
            if (attribute.attributeName().equals(name)) {
                System.err.println(String.format("Error: Cant add attribute [ %s ] because it already exists in table", name));
                return false;
            }
        }
        this.Attributes.add(new Attribute(name, type));
        AttribIdxs.put(name, Attributes.size() - 1);
        return true;
    }

    public void setForeignKeys(ArrayList<ForeignKey> foreignKeys) {
        ForeignKeys = foreignKeys;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public void setAttributes(ArrayList<Attribute> attributes) {

        Attributes = attributes;
        // mapping the attribute name to the idx of that attribute  needed for later
        HashMap<String, Integer> attributeNameToIdx = new HashMap<>();
        for (int i = 0; i < Attributes.size(); i++) {
            attributeNameToIdx.put(Attributes.get(i).getAttributeName(), i);
        }
        this.AttribIdxs = attributeNameToIdx;
    }

    public void setPagesThatBelongToMe(ArrayList<Integer> pagesThatBelongToMe) {
        PagesThatBelongToMe = pagesThatBelongToMe;
    }

    public void setPrimaryKey(Attribute primaryKey) {
        PrimaryKey = primaryKey;
    }

    @Override
    public boolean dropAttribute(String name) {

        int idx = 0;

        for (Attribute attribute : Attributes) {

            // we that attribute is in the table
            if (attribute.attributeName().equals(name)) {

                // re move it
                this.Attributes.remove(idx);

                // drop the index if it has one on this attribute
                IndexedAttributes.remove(attribute.attributeName());

                // rebuild and set the hash table of name to index
                HashMap<String, Integer> attributeNameToIdx = new HashMap<>();
                for (int i = 0; i < Attributes.size(); i++) {
                    attributeNameToIdx.put(Attributes.get(i).getAttributeName(), i);
                }
                this.AttribIdxs = attributeNameToIdx;


                return true;
            }
            idx++;
        }
        System.err.println(String.format("Error: Cant drop attribute [ %s ] because it does not exist in the table", name));
        return false;
    }

    @Override
    public boolean addForeignKey(ForeignKey fk) {

        // test if fk already exists
        for (ForeignKey foreignKey : ForeignKeys) {
            if (foreignKey.toString().equals(fk.toString())) {
                System.err.println("Error: Cant add ForeignKey because it already exists");
                return false;
            }
        }
        this.ForeignKeys.add(fk);
        return true;
    }

    public void addPageAffiliations(int pageName) {
        this.PagesThatBelongToMe.add(pageName);
    }

    @Override
    public boolean addIndex(String attributeName) {

        if (IndexedAttributes.containsKey(attributeName)) {
            System.err.println(attributeName + " is already indexed");
            return false;
        }

        // check attribute exits in table
        if (AttribIdxs.containsKey(attributeName)) {

            // make new b+ tree with that attrib


            var newTree = BPlusTree.TreeFromTableAttribute(this, AttribIdxs.get(attributeName));

            IndexedAttributes.put(attributeName, newTree);


            return true;
        }
        System.err.println("cant make index on attribute because " + attributeName + " does not exist");
        return false;
    }

    /*


    structure [int:numTotaltables, int:tableNameLength, String: tableName, int:tableID
              int:#ofAtributes,    int:attributeLen,    Strings: AttributeDatas..... ,
              int:PkLen,           String: PkAttribute, int: #of Fk's ,    int: lenFk,
              String Fk,           int: #ofPageNames,   ints:pagenames .....         ]
     */
    public byte[] toBytes() {

        try {


            // byte array
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // WRITE table details

            // total number of tables.txt
            outputStream.write(ByteBuffer.allocate(4).putInt(((Catalog) Catalog.getCatalog()).getNumberOFtables()).array());

            VerbosePrint.print(numTables);


            // write out the indicesOfNotNullAttributes

            // the number of indices stored
            outputStream.write(ByteBuffer.allocate(4).putInt(indicesOfNotNullAttributes.size()).array());
            // storing of the indices
            for (Integer index : indicesOfNotNullAttributes) {
                outputStream.write(ByteBuffer.allocate(4).putInt(index).array());
            }


            // tables.txt name len
            outputStream.write(ByteBuffer.allocate(4).putInt(this.TableName.length()).array());
            // table name
            outputStream.write(this.TableName.getBytes());
            // table id
            outputStream.write(ByteBuffer.allocate(4).putInt(this.ID).array());

            // number of attributes we need to read in
            int numOfAttributes = this.Attributes.size();
            outputStream.write(ByteBuffer.allocate(4).putInt(numOfAttributes).array());

            // for each attribute write it out
            for (Attribute attrib : this.Attributes) {
                String StringAtt = attrib.toString();
                int StringAttLen = StringAtt.length();
                // write how log the attribute is
                outputStream.write(ByteBuffer.allocate(4).putInt(StringAttLen).array());
                // write attribute str
                outputStream.write(StringAtt.getBytes());
            }


            // write primary key (atribute)
            String pkString = this.PrimaryKey.toString();
            int pkStringLen = pkString.length();
            outputStream.write(ByteBuffer.allocate(4).putInt(pkStringLen).array());
            outputStream.write(pkString.getBytes());


            // write forigen key
            // number of attributes we need to read in
            int numOfFks = this.ForeignKeys.size();
            outputStream.write(ByteBuffer.allocate(4).putInt(numOfFks).array());

            // for each fk write it out
            for (ForeignKey FK : this.ForeignKeys) {
                //""
                int StringFkLen = FK.getAttrName().length() + FK.refTableName().length() + FK.getRefAttribute().length() + 2;

                String StringFK = FK.getAttrName() + " " + FK.getRefTableName() + " " + FK.getRefAttribute();

                // write how log the fk is
                outputStream.write(ByteBuffer.allocate(4).putInt(StringFkLen).array());
                // write attribute str
                outputStream.write(StringFK.getBytes());
            }
            // write pages that belong to this table

            // write pages that belong to me
            // number of page names we need to write in
            int numOfpageNames = this.PagesThatBelongToMe.size();
            outputStream.write(ByteBuffer.allocate(4).putInt(numOfpageNames).array());

            // for each fk write it out
            for (int pageName : this.PagesThatBelongToMe) {
                // write page name
                outputStream.write(ByteBuffer.allocate(4).putInt(pageName).array());
            }
//            VerbosePrint.print(outputStream.toByteArray().length);
            return outputStream.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
            return null;

        }
    }

    public static ArrayList<ITable> ReadAllTablesFromDisk() {
        try {
            VerbosePrint.print("reading tables.txt from disk");


            // read in streams
            DataInputStream dataInputStr = FileSystem.createCatTabsDataInStream();


            // reading all the tables.txt from page from disk

            // first thing thats stored in a page is the num of tables.txt
            int numTables = 0;
            try {
                numTables = dataInputStr.readInt();

                VerbosePrint.print("-------------[" + numTables + "]--------------");

            } catch (IOException i) {
                VerbosePrint.print("no tables found stored in DB");
                return null;
            }
            Table.numTables = numTables;

            // all tabls in the DB
            ArrayList<ITable> tables = new ArrayList<>();
            for (int tn = 0; tn < numTables; tn++) {

                // the number of indices stored for not null indices
                int numberOfIdxs = dataInputStr.readInt();
                // storing of the indices
                Set<Integer> indices = new HashSet<>();

                for (int ni = 0; ni < numberOfIdxs; ni++) {
                    indices.add(dataInputStr.readInt());
                }

                int tableNameLength = dataInputStr.readInt();
                String tableName = new String(dataInputStr.readNBytes(tableNameLength));
                int tableID = dataInputStr.readInt();


                // get attributes
                int NumOfAtributes = dataInputStr.readInt();
                ArrayList<Attribute> tableAttributes = new ArrayList<>();
                for (int an = 0; an < NumOfAtributes; an++) {
                    int attributeLen = dataInputStr.readInt();
                    String attribute = new String(dataInputStr.readNBytes(attributeLen));
                    String[] attribVals = attribute.split(" ");

                    tableAttributes.add(new Attribute(attribVals[0], attribVals[1]));
                }


                // mk Pkeys
                int PKattributeLen = dataInputStr.readInt();
                String PKattribute = new String(dataInputStr.readNBytes(PKattributeLen));
                String[] PKVals = PKattribute.split(" ");
                Attribute PK = new Attribute(PKVals[0], PKVals[1]);


                // mk fkeys
                ArrayList<ForeignKey> ForeignKeys = new ArrayList<>();
                int numOfFk = dataInputStr.readInt();
                if (numOfFk > 0) {
                    for (int fn = 0; fn < numOfFk; fn++) {
                        int lenFk = dataInputStr.readInt();
                        String[] fk = new String(dataInputStr.readNBytes(lenFk)).split(" ");
                        String attrName = fk[0];
                        String refTableName = fk[1];
                        String refAttribute = fk[2];


                        ForeignKeys.add(new ForeignKey(refTableName, refAttribute, attrName));

                    }
                }

                // get pages
                int numPagesThatBelongToMe = dataInputStr.readInt();
                ArrayList<Integer> BelongToMe = new ArrayList<>();
                Page.numPages += numPagesThatBelongToMe;

                if (numPagesThatBelongToMe > 0) {

                    for (int an = 0; an < numPagesThatBelongToMe; an++) {
                        int pageName = dataInputStr.readInt();
                        BelongToMe.add(pageName);
                    }
                }

                //make table and add it to tables

                Table DiskTable = new Table(tableName, tableAttributes, PK, BelongToMe);
                DiskTable.setForeignKeys(ForeignKeys);
                DiskTable.setNotNullIdxs(indices);
                DiskTable.setID(tableID);
                tables.add(DiskTable);

                // not needed but have to do it to read correct bytes

                if (tn < numTables - 1) {
                    dataInputStr.readInt();
                }

            }
            return tables;

        } catch (IOException e) {
            e.printStackTrace();

            System.err.println("IO Error reading table from disk");
            return null;
        }
    }


    // indexs in schema that cant be null


    public void setNotNullIdxs(Set<Integer> notNullIndexs) {
        this.indicesOfNotNullAttributes = (HashSet<Integer>) notNullIndexs;
    }

    public int getMaxAttributeSize() {
        int Max = 0;
        for (Attribute attribute : Attributes) {

            String attributeType = attribute.getAttributeType().toLowerCase();


            int attributeSize = 0;

            switch (attributeType) {
                case "integer":
                    attributeSize = 4;
                    break;
                case "double":
                    attributeSize = 8;
                    break;
                case "boolean":
                    attributeSize = 1;
                    break;
                default:

                    attributeSize = Integer.parseInt(attributeType.substring(attributeType.indexOf("(") + 1, attributeType.length() - 1));


            }
            Max = Math.max(Max, attributeSize);


        }
        return Max;
    }

    public BPlusTree getPkTree() {
        return IndexedAttributes.get(Attributes.get(pkIdx()).getAttributeName());
    }

    public void SetPagesThatBelongToMe(ArrayList<Integer> pagesThatBelongToMe) {
        this.PagesThatBelongToMe = pagesThatBelongToMe;
    }

    public void SetAttribIdxs(HashMap<String, Integer> attribIdxs) {

        this.AttribIdxs = attribIdxs;
    }

    public void SetIndicesOfNotNullAttributes(HashSet<Integer> indicesOfNotNullAttributes) {
        this.indicesOfNotNullAttributes = indicesOfNotNullAttributes;
    }

    public void SetAttributes(ArrayList<Attribute> attributes) {
        this.Attributes = attributes;
    }

    public boolean dropAttributeCartTable(Table table, String name, int attrIndex) {


        try {
            if (attrIndex >= table.getAttributes().size()) {
                System.err.println("Table cant remove attribute at index " + attrIndex + " because" +
                        " table doesn't have that many attributes");
                return false;
            }

            // get the pages that belong to this table
            ArrayList<Integer> pages = ((Table) table).getPagesThatBelongToMe();

            // get the first table in the table
            int headPtr = pages.get(0);

            // clone the old table

            Table Clone = new Table(table);

            // remove the atrribute attribute because we need the old schema to read in the old table pages
            Clone.dropAttribute(table.getAttributes().get(attrIndex).getAttributeName());


            // clear the clone table pages (we will make new ones)
            Clone.getPagesThatBelongToMe().clear();

            // make a new first page for the clone table and add it to clone table
            Page firstPageForTable = new Page(Clone);


            firstPageForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), Clone);

            //tree for tHIS TABLE  new empty table
            var newTree = BPlusTree.TreeFromTableAttribute(Clone, Clone.pkIdx());

            Clone.IndexedAttributes.put(table.getAttributes().get(Clone.pkIdx()).getAttributeName(), newTree);


            // looping though all the pages of the old table
            while (headPtr != -1) {


                // load the page from memory
                Page headPage = ((StorageManager) StorageManager
                        .getStorageManager())
                        .getPagebuffer()
                        .getPageFromBuffer(String.valueOf(headPtr), table);


                // cloning old table page records
                ArrayList<ArrayList<Object>> newRecs = new ArrayList<>();
                for (ArrayList<Object> row : headPage.getPageRecords()) {
                    newRecs.add(new ArrayList<>(row));
                }

                // adding new attribute/value to each row
                // these will be the clone table new records

                newRecs.forEach(row -> row.remove(attrIndex));


                // reinsert new recs into new table with the updated schema
                for (ArrayList<Object> tempRec : newRecs) {
                    StorageManager.getStorageManager().insertRecord(Clone, tempRec);
                }


                // getting next page from old table

                // mem optimization remove the old page from disk
                // and not wait till end to remove all the page that way we would only ever have one
                // page duplicate vs an entire table duped in memory at the end

//                DeletePageFromTable(table, headPtr);

                headPtr = headPage.getPtrToNextPage();

            }

//            clearTablesPages(table);
            // set new table name to old table

            ((Table) table).SetAttributes(Clone.getAttributes());
            ((Table) table).SetIndicesOfNotNullAttributes(Clone.indicesOfNotNullAttributes);
            ((Table) table).SetPagesThatBelongToMe(Clone.getPagesThatBelongToMe());
            ((Table) table).SetAttribIdxs(Clone.AttribIdxs);
            ((Table) table).IndexedAttributes = Clone.IndexedAttributes;

            return true;


        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("failure adding attribute from table " + table.getTableName());
            return false;
        }


    }
}
