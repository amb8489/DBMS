package common;
import catalog.ACatalog;
import catalog.Catalog;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/*
  Implementation of the ITable interface.  The interface
  @author Kyle Ferguson (krf6081@rit.edu)
  @author Aaron Berghash (amb8489@rit.edu)

 */

public class Table implements ITable{

    public static int numTables = 0; // tracks how many tables.txt have been created; used to establish table ID
    private String TableName;
    private int ID;
    private Attribute PrimaryKey;
    private ArrayList<Attribute> Attributes;
    private ArrayList<ForeignKey> ForeignKeys= new ArrayList<>();
    private ArrayList<Integer>PagesThatBelongToMe = new ArrayList<>();


    // ADD INDEX LIST HERE - FOURTH PHASE



    public Table(String name,ArrayList<Attribute> Attributes,Attribute PrimaryKey ){
        ID = numTables;
        numTables++;
        this.Attributes = Attributes;
        this.TableName = name;
        this.PrimaryKey = PrimaryKey;

        Page firstPAgeForTable = new Page(this);
        firstPAgeForTable.writeToDisk(ACatalog.getCatalog().getDbLocation(), this);

    }

    public int pkIdx(){
        int idx = 0;
        for(Attribute Attrib:Attributes){
            if (Attrib.equals(this.PrimaryKey)){
                return idx;
            }
            idx++;
        }
        return -1;
    }

    public Table(String tableName, ArrayList<Attribute> tableAttributes, Attribute pk, ArrayList<Integer> belongToMe) {

        ID = numTables;
//        numTables++;
        this.Attributes = tableAttributes;
        this.TableName = tableName;
        this.PrimaryKey = pk;
        this.PagesThatBelongToMe = belongToMe;
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

        for(Attribute attribute:Attributes) {
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
        for(Attribute attribute:Attributes){
            if (attribute.attributeName().equals(name)){
                System.err.println(String.format("Error: Cant add attribute [ %s ] because it already exists in table",name));
                return false;
            }
        }
        this.Attributes.add( new Attribute(name,type));
        // TODO tell the storage manager to add the attribute to the data stored in the table. (later phase)

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

        for(Attribute attribute:Attributes){
            if (attribute.attributeName().equals(name)){
                this.Attributes.remove(idx);
                // TODO tell the storage manager to drop the attribute from the data stored in the table. (later phase)
                return true;
            }
            idx++;
        }
        System.err.println(String.format("Error: Cant drop attribute [ %s ] because it does not exist in the table",name));
        return false;
    }

    @Override
    public boolean addForeignKey(ForeignKey fk) {

        // test if fk already exists
        for(ForeignKey foreignKey :ForeignKeys){
            if (foreignKey.getAttrName().equals(fk.getAttrName())){
                System.err.println("Error: Cant add ForeignKey because it already exists");
                return false;
            }
        }
        this.ForeignKeys.add(fk);
        return true;
    }

    public void addPageAffiliations(int pageName){
        this.PagesThatBelongToMe.add(pageName);
    }

    @Override
    public boolean addIndex(String attributeName) {
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
            outputStream.write(ByteBuffer.allocate(4).putInt(Table.numTables).array());
            System.out.println(numTables);

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
            for (Attribute attrib:  this.Attributes) {
                String StringAtt = attrib.toString();
                int StringAttLen =  StringAtt.length();
                // write how log the attribute is
                outputStream.write(ByteBuffer.allocate(4).putInt(StringAttLen).array());
                // write attribute str
                outputStream.write(StringAtt.getBytes());
            }


            // write primary key (atribute)
            String pkString = this.PrimaryKey.toString();
            int pkStringLen =  pkString.length();
            outputStream.write(ByteBuffer.allocate(4).putInt(pkStringLen).array());
            outputStream.write(pkString.getBytes());


            // write forigen key
            // number of attributes we need to read in
            int numOfFks = this.ForeignKeys.size();
            outputStream.write(ByteBuffer.allocate(4).putInt(numOfFks).array());

            // for each fk write it out
            for (ForeignKey FK:  this.ForeignKeys) {
                String StringFK = FK.toString();
                int StringFkLen =  StringFK.length();
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
            for (int pageName:  this.PagesThatBelongToMe) {
                // write page name
                outputStream.write(ByteBuffer.allocate(4).putInt(pageName).array());
            }
//            System.out.println(outputStream.toByteArray().length);
            return outputStream.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
            return null;

        }
    }

    public static ArrayList<ITable> ReadAllTablesFromDisk(String DBlocation) {
        String location = DBlocation+"/tabs/tables.txt";

        try {
            System.out.println("reading tables.txt from disk");


            // read in streams
            FileInputStream inputStream;
            inputStream = new FileInputStream(location);
            DataInputStream dataInputStr = new DataInputStream(inputStream);


            // reading all the tables.txt from page from disk

            // first thing thats stored in a page is the num of tables.txt
            int numTables = 0;
            try {
                numTables = dataInputStr.readInt();
                System.out.println("-------------["+numTables+"]--------------");

            }catch (IOException i){
                System.out.println("no tables found stored in DB");
                return null;
            }
            Table.numTables = numTables;
//            System.exit(1);

            ArrayList<ITable> tables = new ArrayList<>();
            for (int tn = 0; tn < numTables; tn++) {
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

                    tableAttributes.add(new Attribute(attribVals[0],attribVals[1]));
                }


                // mk Pkeys
                int PKattributeLen = dataInputStr.readInt();
                String PKattribute = new String(dataInputStr.readNBytes(PKattributeLen));
                String[] PKVals = PKattribute.split(" ");
                Attribute PK = new Attribute(PKVals[0],PKVals[1]);


                // mk fkeys
                ArrayList<ForeignKey> ForeignKeys= new ArrayList<>();
                int numOfFk = dataInputStr.readInt();
                if(numOfFk>0) {
                    for (int fn = 0; fn < numOfFk; fn++) {
                        int lenFk = dataInputStr.readInt();
                        String Fk = new String(dataInputStr.readNBytes(PKattributeLen));

                        System.out.println("hello, this still needs to be finished :) -table");
                        // TODO mk fk

                        // TODO add fk to ForeignKeys
                    }
                }

                // get pages
                int numPagesThatBelongToMe = dataInputStr.readInt();
                ArrayList<Integer> BelongToMe = new ArrayList<>();
                if(numPagesThatBelongToMe>0) {

                    for (int an = 0; an < numPagesThatBelongToMe; an++) {
                        int pageName = dataInputStr.readInt();
                        BelongToMe.add(pageName);
                    }
                }

                //make table and add it to tables

                Table DiskTable = new Table(tableName,tableAttributes,PK,BelongToMe);
                DiskTable.setForeignKeys(ForeignKeys);
                DiskTable.setID(tableID);
                tables.add(DiskTable);

                // not needed but have to do it to read correct bytes

                if(tn < numTables-1) {
                    dataInputStr.readInt();}

            }
            return tables;

        }catch (IOException e){
            e.printStackTrace();

            System.err.println("IO Error reading table from disk AT "+location);
            return null;
        }
    }
}
