package common;

/**
 * Represents a record pointer in the database.
 *
 * PROVIDED - DO NOT CHANGE THIS FILE
 *
 */
public record RecordPointer(int page, int index) {

    /**
     * The page number to find this record
     */
    @Override
    public int page() {
        return page;
    }

    /**
     * The index on the page to find this record
     */
    @Override
    public int index() {
        return index;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof RecordPointer rp))
            return false;
        return rp.page == this.page && rp.index == this.index;
    }

    @Override
    public String toString(){
        return "R"+index;
    }
}
