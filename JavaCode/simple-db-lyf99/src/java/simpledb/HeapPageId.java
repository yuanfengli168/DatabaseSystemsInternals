package simpledb;

import java.util.Objects;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
    // Jacky's personal fields starts from here
    // 2021/01/19
    private int tableId;
    private int pgNo;


    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        // return Integer.parseInt("" + this.tableId + this.pgNo);   -- old wrong code.
        // one way to work out!!
        // return 17 * this.tableId + pgNo;  // made few mistakes, and this is the answer!! the number is arbitrary,
        // which means 17, 31, will always work!!
        return Objects.hash(pgNo, tableId);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        // edge case when o is even not a heapPageId object
        if (!(o instanceof HeapPageId)) {
            return false;
        }
        // what if o and this is totally equal??
        if (this == o) {
            return true;
        }
        HeapPageId pid = (HeapPageId) o;
        return pid.getTableId() == this.tableId && pid.getPageNumber() == this.pgNo;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
