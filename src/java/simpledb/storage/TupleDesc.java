package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private static final long serialVersionUID = 1L;

    private TDItem[] tdItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with
     * associated named fields.
     * 
     * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must
     *        contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++)
            this.tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified
     * types, with anonymous (unnamed) fields.
     * 
     * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must
     *        contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++)
            this.tdItems[i] = new TDItem(typeAr[i], null);
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are included in this
     *         TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(this.tdItems).iterator();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return this.tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return this.tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int i = 0;
        while (i < tdItems.length) {
            if (name != null && name.equals(tdItems[i].fieldName))
                return i;
            i++;
        }
        throw new NoSuchElementException();

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note that tuples from
     *         a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem tdItem : tdItems)
            size += tdItem.fieldType.getLen();
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered
     * equal if they have the same number of items and if the i-th type in this TupleDesc is equal
     * to the i-th type in o for every i.
     * 
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (!(other instanceof TupleDesc))
            return false;
        TupleDesc o = (TupleDesc) other;
        int numberOfFields = numFields();
        if (numberOfFields != o.numFields())
            return false;
        for (int i = 0; i < numberOfFields; i++) {
            if (!(getFieldType(i) == o.getFieldType(i)))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does
     * not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (TDItem tdItem : this.tdItems) {
            sb.append(tdItem.fieldType.toString());
            sb.append('(');
            sb.append(tdItem.fieldName);
            sb.append("), ");
        }
        if (sb.length() > 0)
            sb.delete(sb.length() - 2, sb.length());
        return sb.toString();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first
     * td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int size = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[size];
        String[] fieldAr = new String[size];
        int i = -1;
        for (TDItem tdItem : td1.tdItems) {
            i++;
            typeAr[i] = tdItem.fieldType;
            fieldAr[i] = tdItem.fieldName;
        }
        for (TDItem tdItem : td2.tdItems) {
            i++;
            typeAr[i] = tdItem.fieldType;
            fieldAr[i] = tdItem.fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
}
