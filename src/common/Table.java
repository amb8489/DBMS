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

    @Override
    public boolean addIndex(String attributeName) {
        return false;
    }

    // TODO
    // ~/Deskt op/DB/catalog/tables
    public boolean saveToDisk(String location) {

        try {


            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(location)));
            // byte array
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // WRITE table details

            //TODO write these out to disk
//            private Attribute PrimaryKey;
//            private ArrayList<Attribute> Attributes;
//            private ArrayList<ForeignKey> ForeignKeys;

            outputStream.write(ByteBuffer.allocate(4).putInt(Table.numTables).array());
            outputStream.write(this.TableName.getBytes());
            outputStream.write(ByteBuffer.allocate(4).putInt(this.ID).array());

            out.write(outputStream.toByteArray());


            return true;

        }catch (FileNotFoundException e) {
            System.err.println("LOCATion NOT FOUND");
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;

        }
    }
}
