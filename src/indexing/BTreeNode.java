package indexing;

import common.RecordPointer;

import java.util.ArrayList;

public class BTreeNode<T> {


    public int id;
    // should be arrays of len max n??
    public ArrayList<T> keys = new ArrayList<T>();
    public ArrayList<BTreeNode<T>> children = new ArrayList<>();
    public ArrayList<RecordPointer> rps = new ArrayList<>();


    public int numKeys = 1;
    public boolean isLeaf = true;
	public BTreeNode<T> parent = null;
    public BTreeNode<T> next = null;
    public int N;





    public BTreeNode(int id,int N){
        this.id = id;
        this.N = N;

        // TODO number of childen is always # of keys + 1
        for (int i = 0; i < N+1; i++,children.add(null));
        for (int i = 0; i < N; i++,keys.add(null));

    }
}

