package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> fields;

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

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {

            if (o == null)
                return false;

            if (o == this)
                return true;

            if (!(o instanceof TDItem))
                return false;

            TDItem item = (TDItem)o;
            return item.fieldType == this.fieldType;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return fields.iterator();
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
        // some code goes here
        assert typeAr.length > 0 : "Can't use empty Type array to init TupleDesc";

        fields = new ArrayList<>();

        int len1 = typeAr.length;
        int len2 = fieldAr.length;

        if (len2 < len1) {
            for (int i = 0; i < len2; i++) {
                TDItem field = new TDItem(typeAr[i], fieldAr[i]);
                fields.add(field);
            }
            for (int i = len2; i < len1; i++) {
                TDItem field = new TDItem(typeAr[i], null);
                fields.add(field);
            }
        } else {
            for (int i = 0; i < len1; i++) {
                TDItem field = new TDItem(typeAr[i], fieldAr[i]);
                fields.add(field);
            }
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
        assert typeAr.length > 0 : "Can't use empty Type array to init TupleDesc";

        fields = new ArrayList<>();

        for (Type type: typeAr) {
            TDItem field = new TDItem(type, null);
            fields.add(field);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fields.size();
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
        if (i >= 0 && i < fields.size())
            return fields.get(i).fieldName;
        else
            throw new NoSuchElementException();
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
        if (i >= 0 && i < fields.size())
            return fields.get(i).fieldType;
        else
            throw new NoSuchElementException();
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
        for (int i = 0; i < fields.size(); i++) {
            TDItem field = fields.get(i);
            if (field.fieldName == null)
                continue;
            if (field.fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int bytes = 0;
        for (TDItem field: fields) {
            bytes += field.fieldType.getLen();
        }
        return bytes;
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
        // some code goes
        final int numFields = td1.numFields() + td2.numFields();

        Type[] types = new Type[numFields];
        String[] names = new String[numFields];

        int index = 0;
        Iterator<TDItem> it1 = td1.iterator();
        while (it1.hasNext()) {
            TDItem field = it1.next();
            types[index] = field.fieldType;
            names[index] = field.fieldName;
            index++;
        }

        Iterator<TDItem> it2 = td2.iterator();
        while (it2.hasNext()) {
            TDItem field = it2.next();
            types[index] = field.fieldType;
            names[index] = field.fieldName;
            index++;
        }

        return new TupleDesc(types, names);
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

    public boolean equals(Object o) {
        // some code goes here
        if (o == null)
            return false;

        if (o == this)
            return true;

        if (!(o instanceof TupleDesc))
            return false;

        TupleDesc tupleDesc = (TupleDesc)o;
        if (tupleDesc.numFields() != this.numFields())
            return false;

        Iterator<TDItem> it = this.iterator();
        Iterator<TDItem> it2 = tupleDesc.iterator();
        while (it.hasNext()) {
            if (!( it.next().equals( it2.next() )))
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
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String res = new String();

        for (int i = 0; i < fields.size(); i++) {
           res += fields.get(i).toString();
           if (i != fields.size()-1)
               res += ", ";
        }

        return res;
    }
}
