package common;

/*
  Interface for table metadata
  PROVIDED - DO NOT CHANGE THIS FILE

  You will make a class called Table that will inherit from this interface.
  Both classes must be in the common package

  @author Scott C Johnson (sxjcs@rit.edu)

 */

import indexing.IBPlusTree;

import java.util.ArrayList;

public interface ITable {

    /**
     * returns the name of this table
     * @return the name of the table
     */
    public String getTableName();

    /**
     * sets the name of the table
     * @param name the name to give the table
     */
    public void setTableName(String name);

    /**
     * returns the id of the table.
     * This should be assigned by the catalog then creating the table.
     * @return
     */
    public int getTableId();

    /**
     * Gets the list of attribute metadata for this table.
     * @return An array list of the attributes
     */
    public ArrayList<Attribute> getAttributes();

    /**
     * Gets an individual attrbiute with the provided name
     * @param name the name of the attribute to get
     * @return the attribute instance with the name, or null if not such attribute exists
     */
    public Attribute getAttrByName(String name);

    /**
     * Gets the attribute that is the primary key of the table.
     * @return
     */
    public Attribute getPrimaryKey();

    /**
     * Gets a list of all foreign keys for this table.
     * @return
     */
    public ArrayList<ForeignKey> getForeignKeys();
    /**
     * Adds the attribute with the provided name and type
     *
     * This only modifies the table metadata; not the data stored in the table.
     *
     * @param name the name of the attribute to add.
     *             Note: names are unique within a table. Duplicate names are an error.
     * @param type the type of the attribute to add.
     * @return true if successful; false otherwise
     */
    public boolean addAttribute(String name, String type);

    /**
     * Drops the attribute with the provided name
     *
     * * This only modifies the table metadata; not the data stored in the table.
     *
     * @param name the name of the attribute to drop
     * @return true if successful; false otherwise.
     */
    public boolean dropAttribute(String name);

    /**
     * Adds a new foreign key to the table.
     * An attrbiute cannot have more than one foreign key that refers to the same table/attribute combination
     * @param fk the foreign key to add
     * @return true if successful; false otherwise
     */
    public boolean addForeignKey(ForeignKey fk);

    /**
     * Will create an index for the attribute with the provided name.
     * An attribute can have only one index.
     *
     * @param the name of the attribute tio add the index on.
     * @return true if successful, false otherwise
     */
    public boolean addIndex(String attributeName);
    
    /**
     * toString to print this table in a common readable format.
     * @return a string representation of this table.
     */
    default String tableToString(){
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[%d] %s( ", getTableId(), getTableName()));
        sb.append("\n");
        sb.append("\t").append(getAttributes().get(0));
        for(int i = 1; i < getAttributes().size(); i++){
            sb.append(",\n");
            sb.append(getAttributes().get(i));
        }

        sb.append(String.format(",\n\tprimarykey(%s)", getPrimaryKey().attributeName()));

        for(ForeignKey fk: getForeignKeys()){
            sb.append(",\n\t");
            sb.append(fk.toString());
        }

        sb.append("\n)");
        return sb.toString();
    }


}
