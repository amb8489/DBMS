package indexing;

import common.RecordPointer;

import java.util.ArrayList;

/**
 * Interface for B+ Tree Nodes.
 * <p>
 * Each node must be stored as a page on the file system.
 * <p>
 * Nodes will store search keys and page numbers.
 * <p>
 * See the writeup for more details
 *
 * @param <K> the data type of the search key.
 *            <p>
 *            Pointers will be to pages; not to other nodes.
 */

public interface IBPlusTree<K> {

    /**
     * Will insert the provided record pointer in the proper place in to the B+Tree for the
     * provided search key
     * <p>
     * Note multiple record pointers can have the same search key
     *
     * @param rp        the record pointer to insert.
     * @param searchKey the search key of the record pointer.
     * @return true if successful, false otherwise
     */
    public boolean insertRecordPointer(RecordPointer rp, K searchKey);

    /**
     * Will delete the provided record pointer in the proper place in to the B+Tree for the
     * provided search key
     * <p>
     * Note multiple record pointers can have the same search key
     *
     * @param rp        the record pointer to delete
     * @param searchKey the search key of the record pointer.
     * @return true if successful, false otherwise
     */
    public boolean removeRecordPointer(RecordPointer rp, K searchKey);

    /**
     * Finds all record pointers with the provided search key. They should all be in the same buckets
     *
     * @param searchKey the search key to find record pointers for
     * @return an array list of record pointers represented by the search key.
     * returning null means there was an error
     * returning an empty list means there were no pointers for that search key or the search key did not exist.
     */
    public ArrayList<RecordPointer> search(K searchKey);

    /**
     * Finds all record pointers above/below the provided search key.
     *
     * @param searchKey the search key to find record pointers above/below
     * @param lessThan  get all values less than this searchKey if true, greater than if false
     * @param equalTo   get all the values equal to this search key
     * @return an array list of record pointers represented by the search keys above/below the provided search key.
     * returning null means there was an error
     * returning an empty list means there were no pointers above/below that search key or the search key did not exist.
     */
    public ArrayList<RecordPointer> searchRange(K searchKey, boolean lessThan, boolean equalTo);
}
