package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    // More fields
    private int numFields;
    private List<TDItem> tdItems = new ArrayList<>(); // will implement in future if needed

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;



    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here;
        this.numFields = typeAr.length;  // because fieldAr might be full of null
        for (int i = 0; i < this.numFields; i++) {
            tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }


    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.numFields = typeAr.length;
        for (int i = 0; i < this.numFields; i++) {
            tdItems.add(new TDItem(typeAr[i], "Un_named"));
        }

    }

//    // I created this new constructor by myself.20210111
//    public TupleDesc() {
//        this.numFields = 0;
//    }
//
//    // I created the new constructor again by myself
//    public TupleDesc(Type[] typeAr, String[] fieldAr, int numFields) {
//        this.numFields = numFields;
//    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.numFields) {
            throw new NoSuchElementException();
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i > this.numFields) {
            throw new NoSuchElementException();
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < numFields; i++) {
            if (tdItems.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();

        /* another way to do it
        * int index = 0;
        * Iterator<TDItem> itr = iterator();
        * while (itr.hasNext()) {
        *   TDItem next = itr.next();
        *   if (next.fieldName.equals(name)) {
        *       return index;
        *   }
        *   index++;
        * }
        * throw new NoSuchElementException();
        * */

    }


    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem tdi : tdItems) {
            size += tdi.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int tdTotalNumFields = td1.numFields() + td2.numFields();
        Type[] tdTotalTypeAr = new Type[tdTotalNumFields];
        String[] tdTotalFieldAr = new String[tdTotalNumFields];
        for (int i = 0; i < td1.numFields; i++) {
            tdTotalTypeAr[i] = td1.getFieldType(i);
            tdTotalFieldAr[i] = td1.getFieldName(i);
        }
        // now we paste td2's type and fields in it
        for (int i = td1.numFields; i < tdTotalNumFields; i++) {
            tdTotalTypeAr[i] = td2.getFieldType(i - td1.numFields());
            tdTotalFieldAr[i] = td2.getFieldName(i - td1.numFields());
        }
        return new TupleDesc(tdTotalTypeAr,tdTotalFieldAr);


    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    /*
     * C:\Users\Jacky\OneDrive\Documents\Zoom\2021-01-11 10.55.45 Ying's Office Hour 96803833948
     * */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc t = (TupleDesc)o;
        if (t.numFields() == this.numFields) {
            for (int i = 0; i < this.numFields; i++) {
                if (t.getFieldType(i) != this.tdItems.get(i).fieldType) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here

        String result = "";
        for (int i = 0; i < this.numFields; i++) {
            result += "index" + i + "Field Type: " + this.tdItems.get(i).fieldType + ",";
            result += "index" + i + "Field Name: " + this.tdItems.get(i).fieldName + ";";
        }
        return "(" + result + ")";
    }
}
