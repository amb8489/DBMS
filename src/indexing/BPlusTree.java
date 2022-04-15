package indexing;

import common.RecordPointer;

import java.util.ArrayList;

public class BPlusTree<T extends Comparable<T>> implements IBPlusTree<T> {
    //TODO??
    public int nextIndex = 0;

    public boolean preemptiveSplit = false;
    public int max_keys;
    public int min_keys;
    public int split_index;
    public int TreeNsize;
    public int max_degree;
    public BTreeNode<T> treeRoot = null;


    public BPlusTree(double MaxPageSize, double MaxAttributeSize) {

        this.TreeNsize = (int) (Math.floor(MaxPageSize / (4 + MaxAttributeSize)) - 1);
//        this.nextIndex = 3;



        this.max_degree = TreeNsize;
        this.max_keys = TreeNsize - 1;
        this.min_keys = (int ) Math.floor((TreeNsize + 1) / 2.0) - 1;
        this.split_index = (int) Math.floor((TreeNsize) / 2.0);
        System.out.println(split_index);

    }


    // for testing
    public BPlusTree(int n) {

        this.TreeNsize = n;

//        this.nextIndex = 3;

        this.max_degree = n;
        this.max_keys = n - 1;
        this.min_keys = (int ) Math.floor((n + 1) / 2.0) - 1;
        this.split_index = (int) Math.floor((n) / 2.0);
        System.out.println(split_index);
    }


    @Override
    public boolean insertRecordPointer(RecordPointer rp, T searchKey) {

        return insertElement(rp,searchKey);
    }

    @Override
    public boolean removeRecordPointer(RecordPointer rp, T searchKey) {
        return false;
    }

    @Override
    public ArrayList<RecordPointer> search(T searchKey) {
        return null;
    }

    @Override
    public ArrayList<RecordPointer> searchRange(T searchKey, boolean lessThan, boolean equalTo) {
        return null;
    }


    public boolean insertElement(RecordPointer rp, T insertedValue) {


        if (this.treeRoot == null) {
            this.treeRoot = new BTreeNode<T>(this.nextIndex++,this.TreeNsize);
            this.treeRoot.keys.add(0, insertedValue);
            //TODO add rp
            return true;
        } else {
            return insert(this.treeRoot, insertedValue, rp);

        }


    }

    public boolean insert(BTreeNode<T> tree, T insertValue, RecordPointer rp) {

        //steps
        // search for leaf node where insert belongs
        // insert
        // repair
        if (tree.isLeaf) {
            tree.numKeys++;
            var insertIndex = tree.numKeys - 1;
            while (insertIndex > 0 && IsAGTB(tree.keys.get(insertIndex - 1), insertValue)) {
                tree.keys.set(insertIndex, tree.keys.get(insertIndex - 1));
                insertIndex--;
            }
            tree.keys.add(insertIndex, insertValue);


            return this.insertRepair(tree);

        } else {
            var findIndex = 0;
            while (findIndex < tree.numKeys && IsALTB(tree.keys.get(findIndex), insertValue)) {
                findIndex++;
            }

            return this.insert(tree.children.get(findIndex), insertValue, rp);
        }
    }

    public boolean insertRepair(BTreeNode<T> tree) {
        if (tree.numKeys <= this.max_keys) {
            return true;
            //root
        } else if (tree.parent == null) {
            System.out.println("1 need to repair node "+tree.keys);

            this.treeRoot = this.split(tree);
            return true;
        } else {
            System.out.println("2 need to repair node "+tree.keys);

            BTreeNode<T> newNode = this.split(tree);
            return this.insertRepair(newNode);
        }
    }


    public BTreeNode<T> split(BTreeNode<T> tree) {

        var rightNode = new BTreeNode<T>(this.nextIndex++,this.TreeNsize);

        T risingNode = tree.keys.get(this.split_index);
        System.out.println("risingNode:  "+risingNode);

        int i;
        int parentIndex = 0;
        if (tree.parent != null) {


            BTreeNode<T> currentParent = tree.parent;

            for (; parentIndex < currentParent.numKeys + 1 && currentParent.children.get(parentIndex) != tree; parentIndex++) ;

            if (parentIndex == currentParent.numKeys + 1) {
                throw new Error("Couldn't find which child we were!");
            }
            for (i = currentParent.numKeys; i > parentIndex; i--) {
                currentParent.children.set(i + 1, currentParent.children.get(i));

                currentParent.keys.set(i, currentParent.keys.get(i - 1));
            }
            currentParent.numKeys++;
            currentParent.keys.set(parentIndex, risingNode);


            currentParent.children.set(parentIndex + 1, rightNode);
            rightNode.parent = currentParent;

        }

        int rightSplit;


        // updating the next pointer to leaf
        if (tree.isLeaf) {

            rightSplit = this.split_index;
            rightNode.next = tree.next;
            tree.next = rightNode;
        } else {
            rightSplit = this.split_index + 1;
        }

        rightNode.numKeys = tree.numKeys - rightSplit;

        System.out.println("rightNode.numKeys "+rightNode.numKeys);


        // moving children to right node
        //TODO.......................
        for (i = rightSplit; i < tree.numKeys + 1; i++) {
            System.out.println(" i - r ::"+(i-rightSplit));

            rightNode.children.add(i - rightSplit, tree.children.get(i));




            if (tree.children.get(i) != null) {
                rightNode.isLeaf = false;
                if (tree.children.get(i) != null) {
                    tree.children.get(i).parent = rightNode;
                }
                tree.children.set(i, null);
            }

        }
        for (i = rightSplit; i < tree.numKeys; i++) {
            rightNode.keys.set(i - rightSplit, tree.keys.get(i));
        }
        BTreeNode<T> leftNode = tree;
        leftNode.numKeys = this.split_index;


        if (tree.parent != null) {

            return tree.parent;

            //			if (tree.parent == null)
        } else {
            this.treeRoot = new BTreeNode<T>(this.nextIndex++,this.TreeNsize);
            this.treeRoot.keys.set(0, risingNode);
            this.treeRoot.children.set(0, leftNode);
            this.treeRoot.children.set(1, rightNode);
            leftNode.parent = this.treeRoot;
            rightNode.parent = this.treeRoot;
            // Connection Point
            this.treeRoot.isLeaf = false;
            return this.treeRoot;
        }
    }


    private boolean IsAGTB(T key1, T key2) {
        return key1.compareTo(key2) > 0;
    }

    private boolean IsALTB(T key1, T key2) {
        return key1.compareTo(key2) < 0;
    }


}
