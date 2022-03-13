package common;

/**
 * Used to represent Foreign keys in the database
 * PROVIDED - DO NOT CHANGE THIS FILE
 */

public record ForeignKey(String refTableName, String refAttribute, String attrName) {
    /**
     * The name of the table this fk refers to
     * @return
     */
    public String getRefTableName() {
        return refTableName;
    }

    /**
     * The name of the attribute this fk refers to in the referred table
     * @return
     */
    public String getRefAttribute() {
        return refAttribute;
    }

    /**
     * The name of the attribute in the refferring table
     * @return
     */
    public String getAttrName() {
        return attrName;
    }

    @Override
    public String toString() {
        return String.format("%s references %s(%s)", attrName, refTableName, refAttribute);
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ForeignKey fk)){
            return false;
        }
        return fk.attrName.equals(attrName)
                && fk.refAttribute.equals(refAttribute)
                && fk.refTableName.equals(refTableName);
    }
}
