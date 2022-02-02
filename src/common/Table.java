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

    public Table(){
        ID = numTables;
        numTables++;
    }



    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public void setTableName(String name) {

    }

    @Override
    public int getTableId() {
        return ID;
    }

    @Override
    public ArrayList<Attribute> getAttributes() {
        return null;
    }

    @Override
    public Attribute getAttrByName(String name) {
        return null;
    }

    @Override
    public Attribute getPrimaryKey() {
        return null;
    }

    @Override
    public ArrayList<ForeignKey> getForeignKeys() {
        return null;
    }

    @Override
    public boolean addAttribute(String name, String type) {
        return false;
    }

    @Override
    public boolean dropAttribute(String name) {
        return false;
    }

    @Override
    public boolean addForeignKey(ForeignKey fk) {
        return false;
    }

    @Override
    public boolean addIndex(String attributeName) {
        return false;
    }
}
