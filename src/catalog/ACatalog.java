package catalog;

/*
  This is an abstract class outlining the functionality of the Catalog

  PROVIDED - DO NOT CHANGE THIS FILE

  You will make a class called Catalog that will implement this functionality.
  Your class must inherit from this class.

  This class and your class must be in the catalog package

  More details in the writeup.

  @author Scott C Johnson (sxjcs@rit.edu)
 */
import common.*;

import java.util.ArrayList;

public abstract class ACatalog {

    // Instance of the storage manager to use for this run
    // Only one storage manager can exist at a time
    private static ACatalog catalog = null;

    /**
     * Will return the current instance of the catalog
     * Must be created in a separate step
     * @return the current catalog, or null is none active
     */
    public static ACatalog getCatalog(){
        return catalog;
    }

    /**
     * Will create an instance of a catalog.
     * If there is a database at the provided database location it must recreate the saved catalog
     * for that database. The provided page size will be ignored in this situation and the saved one
     * will be used.
     *
     * If there is no database at the provided location it will create a new catalog.
     *
     * It must store this as the currently active Catalog for the database.
     *
     * @param location the location of the database (assume no trailing /)
     * @param pageSize the size of a page for the database (in bytes)
     * @param pageBufferSize the size of the page buffer for this database
     * @return the catalog created
     */
    public static ACatalog createCatalog(String location, int pageSize, int pageBufferSize){
        catalog = new Catalog(location, pageSize, pageBufferSize);
        return catalog;
    }

    /**
     * return the database location
     * @return the location of the database
     */
    public abstract String getDbLocation();

    /**
     * returns the current page size of the database
     * @return the current page size of the database in bytes
     */
    public abstract int getPageSize();

    /**
     * return the current page buffer size.
     * Note: this is not the current number of pages in the buffer; it is the max
     * @return
     */
    public abstract int getPageBufferSize();

    /**
     * Determines if a table with the provided name already exists in the database
     * @param tableName the name of the table to look for
     * @return true if the table name exists; false otherwise
     */
    public abstract boolean containsTable(String tableName);

    /**
     * Adds the table with the give name, attributes, and primary key to the database.
     *
     * @param tableName the name of the table to add.
     *                  Names are unique, if a table with this name exists, then an error will be reported.
     * @param attributes the list of attributes for this table.
     *                   Attribute names are unique within a table.
     *                   If two attributes have the same name an error will be reported.
     * @param primaryKey the attribute that is to be considered the primary key. This attribute must be in the list
     *                   of provided attributes. If not, an error will be reported.
     * @return the ITable created; or null upon error.
     */
    public abstract ITable addTable(String tableName, ArrayList<Attribute> attributes, Attribute primaryKey);

    /**
     * This will get the table with the provided name. This is table metadata, not the data stored in the table.
     * @param tableName the name of the table to return
     * @return the Itable for the given name; null if error
     */
    public abstract ITable getTable(String tableName);

    /**
     * This will drop the table from the database with the provided name.
     * This includes removing it from the catalog and all data stored in the table.
     * @param tableName the name of the table to drop
     * @return true if the drop is successful; false otherwise
     */
    public abstract boolean dropTable(String tableName);

    /**
     * This will alter the table with the provided name.
     *
     * Must update any table metadata in the catalog and any data stored in the table.
     *
     * If drop == true, the attribute provided will be dropped.
     *     The data stored in the table must be updated to remove the data for this attribute.
     *     Any indices using this attribute must be removed.
     *     Any table with a foreign key reference to this attribute must be updated; fk must be dropped.
     *
     *  if drop == false, the attribute will be added to the table at the end of the attribute list.
     *     The data stored in the table must be updated to add the new attribute with the provided default value
     *     Two attributes in the table cannot have the same name
     * @param tableName the name of the table to alter
     * @param attr the attribute to add/drop
     * @param drop if true drop, if false add
     * @param defaultValue the default value to add when adding an attribute
     * @return true if successful; false otherwise
     */
    public abstract boolean alterTable(String tableName, Attribute attr, boolean drop, Object defaultValue);

    /**
     * Will clear all data stored in the table with the provided name. This includes clearing all indices related
     * to this table. This does not touch any metadata related to the table.
     * @param tableName the name of the table to clear.
     * @return true if successful; false otherwise
     */
    public abstract boolean clearTable(String tableName);

    /**
     * This will add an index to the attribute in the provided table
     *
     * An attribute may have only one index at a time.
     *
     * @param tableName the name of the table to add the index too
     * @param indexName the name given to the index. Index names are unique within a table.
     * @param attrName the name of the attribute to add the index too
     * @return true if successful; false otherwise
     */
    public abstract boolean addIndex(String tableName, String indexName, String attrName);

    /**
     * This will drop an index to the attribute in the provided table
     *
     * @param tableName the name of the table to drop the index from
     * @param indexName the name of the index to drop
     * @return true if successful; false otherwise
     */
    public abstract boolean dropIndex(String tableName, String indexName);

    /**
     * Will write the metadata for the catalog to storage
     *
     * This metadata will be used to restart the catalog.
     *
     * @return true if successful; false otherwise
     */
    public abstract boolean saveToDisk();


}
