package parsers;

import common.Attribute;

import java.util.ArrayList;

public record ResultSet(ArrayList<Attribute> attrs, ArrayList<ArrayList<Object>> results) {

    @Override
    public ArrayList<Attribute> attrs() {
        return attrs;
    }

    @Override
    public ArrayList<ArrayList<Object>> results() {
        return results;
    }
}
