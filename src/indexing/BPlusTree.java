package indexing;

import common.RecordPointer;

import java.util.ArrayList;

public class BPlusTree {

    Double TreeNsize;
    ArrayList<RecordPointer> buckets;


    public BPlusTree(double MaxPageSize,double MaxAttributeSize){


    TreeNsize =  (Math.floor( MaxPageSize / (4 + MaxAttributeSize) ) -1 );

    }








}
