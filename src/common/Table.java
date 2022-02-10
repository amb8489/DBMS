package common;
import javax.imageio.IIOException;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/*
  Implementation of the ITable interface.  The interface
  @author Kyle Ferguson (krf6081@rit.edu)
  @author Aaron Berghash (amb8489@rit.edu)

 */

public class Table implements ITable{

    private static int numTables = 0; // tracks how many tables have been created; used to establish table ID
    private String TableName;
    private int ID;
    private Attribute PrimaryKey;
    private ArrayList<Attribute> Attributes;
    private ArrayList<ForeignKey> ForeignKeys;
    private ArrayList<Integer>PagesThatBelongToMe;

    // ADD INDEX LIST HERE - FOURTH PHASE

    public Table(String name,ArrayList<Attribute> Attributes,Attribute PrimaryKey ){
        ID = numTables;
        numTables++;
        this.Attributes = Attributes;
        this.TableName = name;
        this.PrimaryKey = PrimaryKey;
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

    private boolean addPageAffiliations(int pageName){
            this.PagesThatBelongToMe.add(pageName);
            return true;
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
    public boolean saveToDisk(String location) {

        try {


            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(location)));
            // byte array
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // WRITE table details
//            private ArrayList<ForeignKey> ForeignKeys;

            outputStream.write(ByteBuffer.allocate(4).putInt(Table.numTables).array());
            outputStream.write(ByteBuffer.allocate(4).putInt(this.TableName.length()).array());
            outputStream.write(this.TableName.getBytes());
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

            // writee to page
            out.write(outputStream.toByteArray());

            return true;

        } catch (IOException e) {
            e.printStackTrace();
            return false;

        }
    }
}
