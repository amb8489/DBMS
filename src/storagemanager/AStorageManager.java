package storagemanager;

/*
  This is an abstract class outlining the functionality of the Storage Manager

  PROVIDED - DO NOT CHANGE THIS FILE

  You will make a class called StorageManager that will implement this functionality.
  Your class must inherit from this class.

  This class and your class must be in the storagemanager package

  More details in the writeup.

  @author Scott C Johnson (sxjcs@rit.edu)
 */

import common.Attribute;
import common.ITable;
import common.RecordPointer;

import java.util.ArrayList;

public abstract class AStorageManager {

    // Instance of the storage manager to use for this run
    // Only one storage manager can exist at a time
    private static AStorageManager sm = null;

    /**
     * Will return the current instance of the storage manager
     * Must be created in a separate step
     * @return the current storage manage, or null is none active
     */
    public static AStorageManager getStorageManager() {
        return sm;
    }

    /**
     * Will create an instance of the storage manager
     * Each case will start with an empty page buffer
     *
     * It must set this as the currently active storage manager for this database.
     *
     * @return the created storage manager, or null upon error
     */
    public static AStorageManager createStorageManager(){
            sm = new StorageManager();
            return sm;
    }
    /**
     * Drops the table with the given id from the storage manager.
     * Storage manager is responsible for dropping the table to the catalog
     * Storage manager is responsible for deleting all data/pages related to the table
     * @param table the table to drop
     * @return true if successfully dropped, false otherwise.
     */
    public abstract boolean clearTableData(ITable table);

    /**
     * Gets a single record for the table with the provided key value
     * @param table the table to get the record from
     * @param pkValue the value of the primary key
     * @return the record if found; otherwise null
     */
    public abstract ArrayList<Object> getRecord(ITable table, Object pkValue);

    /**
     * Gets all records for the provided table.
     *
     * Data must be returned in primary key order
     *
     * @param table the table to get the records for
     * @return the records for the table
     *         NOTE: This can be very large.
     */
    public abstract ArrayList<ArrayList<Object>> getRecords(ITable table);

    /**
     * Inserts the provided record into the requested table
     *
     * May have to update the index of other entries if they are moved to a split page
     *
     * Assumes all data/types have been validated, except for duplicate primary keys. This includes any
     * foreign keys.
     *
     * Data must be stored in primary key order.
     *
     * @param table the table to insert the data into
     * @param record the record to insert
     * @return true if the record was inserted; false otherwise
     */
    public abstract boolean insertRecord(ITable table, ArrayList<Object> record);

    /**
     * deletes the record with the provided key value
     * there should only be one record that meets the condition
     *
     * This assumes and foreign key violations have been handled.
     *
     * @param table the table to delete the record from
     * @param primaryKey the value of the primary key of the record to delete
     * @return true if the record was deleted; false otherwise
     */
    public abstract boolean deleteRecord(ITable table, Object primaryKey);

    /**
     * Updates the provided old record data with the new record data.
     * NOTE: This is not as simple as overwriting. The new record may have a larger size and cause a page overfull.
     *
     * Assumes all data/types have been validated, except for duplicate primary keys. This includes any
     * foreign keys.
     *
     * This assumes and foreign key violations have been handled.
     *
     * May have to update the index of other entries if they are moved to a split page
     *
     * @param table the table to update the record in
     * @param oldRecord the old record data
     * @param newRecord the new record data
     * @return true if the record was updated; false otherwise
     */
    public abstract boolean updateRecord(ITable table, ArrayList<Object> oldRecord, ArrayList<Object> newRecord);

    /**
     * This will purge the page buffer.
     * If the database is shutdown without calling this all data changes in the buffer will be lost.
     */
    public abstract void purgePageBuffer();

    /**
     * Adds the provided default value to the end of all record in the provided table.
     * @param table the table to add the attribute value to
     * @param defaultValue the default value to add
     * @return true if successful; false otherwise
     */
    public abstract boolean addAttributeValue(ITable table, Object defaultValue);

    /**
     * drops the attribute value at the provided index in the provided table.
     *
     * All other attribute values will be shifted down
     *
     * @param table the table to drop the attribute value from
     * @param attrIndex the index of the attrbute value to drop.
     * @return true if successful; false otherwise
     */
    public abstract boolean dropAttributeValue(ITable table, int attrIndex);
}
