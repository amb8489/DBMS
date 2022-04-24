package common;

/**
 * Record used to represent an attribute in the database.
 * MUST stay in the common package
 * PROVIDED - DO NOT CHANGE THIS FILE
 *
 * @author Scott C Johnson (sxjcs@rit.edu)
 */

public record Attribute(String attributeName, String attributeType) {

    /**
     * The name of the attribute
     */
    public String getAttributeName() {
        return attributeName;
    }

    /**
     * The type of the attribute
     */
    public String getAttributeType() {
        return attributeType;
    }

    /**
     * Used to print this attribute is a readable way
     *
     * @return
     */
    @Override
    public String toString() {
        return String.format("%s %s",
                getAttributeName(),
                getAttributeType());
    }

    /**
     * Considered equal if the names are the same.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Attribute attr)) {
            return false;
        }
        return attr.attributeName.equals(this.attributeName);
    }
}
