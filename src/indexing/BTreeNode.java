package indexing;

import common.RecordPointer;

import java.util.ArrayList;

public class BTreeNode<T> {


    public int id;
    public int N;
    public int numKeys = 1;
    public boolean isLeaf = true;
    public BTreeNode<T> parent = null;
    public BTreeNode<T> next = null;


    public ArrayList<T> keys = new ArrayList<T>();
    public ArrayList<BTreeNode<T>> children = new ArrayList<>();
    public ArrayList<RecordPointer> rps = new ArrayList<>();


    public BTreeNode(int id, int N) {
        this.id = id;
        this.N = N;

        for (int i = 0; i < N + 1; i++, children.add(null)) ;
        for (int i = 0; i < N; i++, keys.add(null)) ;
        for (int i = 0; i < N + 1; i++, rps.add(null)) ;

    }


    public String writeOut() {

        String out = " <";
        out += id + " ";
        out += N + " ";

        out += numKeys + " ";
        out += (isLeaf ? 1 : 0) + " ";

        // parent node id
        if (this.parent == null) {
            out += -1 + " ";
        } else {
            out += this.parent.id + " ";
        }

        // next id
        if (this.next == null) {
            out += -1 + " ";
        } else {
            out += this.next.id + " ";
        }


        // writing childs
        out += "{C} ";
        for (int i = 0; i < N + 1; i++) {
            if (children.get(i) != null) {
                out += children.get(i).id + " ";
            }
        }
        out += "{CE} ";

        // writing keys
        out += "{K} ";
        for (int i = 0; i < numKeys; i++) {
            if (keys.get(i) != null) {
                out += keys.get(i) + " ";
            }
        }
        out += "{KE} ";


        // writing rp if leaf
        if (isLeaf) {
            out += "{R} ";
            for (int i = 0; i < numKeys; i++) {
                if (rps.get(i) != null) {
                    out += "(" + rps.get(i).page() + " " + rps.get(i).index() + ") ";
                    ;
                }
            }
            out += "{RE} ";

        }
        out += " >";
        return out;
    }
}

