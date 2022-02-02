package common;
import java.util.ArrayList;

/*
  Implementation of the ITable interface.  The interface


  @author Kyle Ferguson (krf6081@rit.edu)

 */

public class Table implements ITable{
    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public void setTableName(String name) {

    }

    @Override
    public int getTableId() {
        return 0;
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
