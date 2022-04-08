package common;

import java.util.ArrayList;

public class BPTNode {

    // nSize
    int Nsize;

    // page
    String pageName;

    // is  leaf a node
    Boolean isLeaf = false;

    // child nodes
    ArrayList<BPTNode>childNodes;

    // buckets for leaf nodes
    ArrayList<Bucket>buckets = null;


    public BPTNode(int Nsize){
        this.Nsize = Nsize;
    }


}
