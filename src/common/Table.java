package common;
import java.util.ArrayList;

/*
  Implementation of the ITable interface.  The interface


  @author Kyle Ferguson (krf6081@rit.edu)

 */

public class Table implements ITable{

    private static int numTables = 0; // tracks how many tables have been created; used to establish table ID

    private String name;
    private int ID;
    private Attribute PrimaryKey;
    private ArrayList<Attribute> Attributes;
    private ArrayList<ForeignKey> ForeignKeys;
    // ADD INDEX LIST HERE - FOURTH PHASE

    public Table(String name,ArrayList<Attribute> Attributes,Attribute PrimaryKey ){
        ID = numTables;
        numTables++;
        this.Attributes = Attributes;
        this.name = name;
        this.PrimaryKey = PrimaryKey;
    }



    @Override
    public String getTableName() {
        return this.name;
    }

    @Override
    public void setTableName(String name) {
        this.name = name;
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
                return false;
            }
        }
        this.Attributes.add( new Attribute(name,type));
        return true;
    }

    @Override
    public boolean dropAttribute(String name) {

        int idx = 0;

        for(Attribute attribute:Attributes){
            if (attribute.attributeName().equals(name)){
                this.Attributes.remove(idx);
                return true;
            }
            idx++;
        }
        return false;
    }

    @Override
    public boolean addForeignKey(ForeignKey fk) {
        return this.ForeignKeys.add(fk);
    }

    @Override
    public boolean addIndex(String attributeName) {
        return false;
    }
}
