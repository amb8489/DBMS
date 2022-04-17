package indexing;

import common.RecordPointer;

import java.util.ArrayList;

//------------- TODO's ------------------

//TODO comment up code

//TODO STORE/RESORE TO/FROM MEMORY--

//TODO put throws on all functions

//TODO do we even need nextIndex??? i dont think so

//TODO ADD GETTING AND PUTTING RECORD PTS

//TODO TESTING

//TODO give a table a b+ tree for its attributes, mk tree for pk always

// QUESTION : how does a page spit effect the tree ?

//-------------------------------------


public class BPlusTree<T extends Comparable<T>> implements IBPlusTree<T> {
    public int nextIndex = 0;

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
        this.min_keys = (int) Math.floor((TreeNsize + 1) / 2.0) - 1;
        this.split_index = (int) Math.floor((TreeNsize) / 2.0);
        System.out.println(split_index);

    }


    // for testing
    public BPlusTree(int n) {
        this.TreeNsize = n;
        this.max_degree = n;
        this.max_keys = n - 1;
        this.min_keys = (int) Math.floor((n + 1) / 2.0) - 1;
        this.split_index = (int) Math.floor((n) / 2.0);
        System.out.println(split_index);
    }


    @Override
    public boolean insertRecordPointer(RecordPointer rp, T searchKey) {

        return insertElement(rp, searchKey);
    }

    @Override
    public boolean removeRecordPointer(RecordPointer rp, T searchKey) {

        return deleteElement(rp, searchKey);
    }

    @Override
    public ArrayList<RecordPointer> search(T searchKey) {
        //TODO RETURN RPS
        System.out.println(findElemen(searchKey));
        return null;
    }

    // gets the left most leaf
    public BTreeNode<T> getStartingLeaf(BTreeNode<T> ROOT) {
        if (ROOT != null) {
            if (ROOT.isLeaf) {
                return ROOT;
            } else {
                return getStartingLeaf(ROOT.children.get(0));
            }
        }
        return null;
    }

    @Override
    public ArrayList<RecordPointer> searchRange(T searchKey, boolean lessThan, boolean equalTo) {
        try {


            ArrayList<RecordPointer> found = new ArrayList<>();
            if (equalTo) {
                return search(searchKey);
            } else if (lessThan){


                var curr = getStartingLeaf(treeRoot);

                if (curr.numKeys > 0) {

                    // getting the first val of node
                    int idx = 0;
                    T val = curr.keys.get(idx);

                    // while each value on leaf if < search key add to found
                    // might not be true at first val in node but will be at some point in the leaf

                    while (IsALTB(val, searchKey)) {

                        //TODO add rp to found
                        System.out.print(val+" ");

                        // get next value



                        // if we are out of values for thus leaf go to next leaf
                        if (idx+1 == curr.numKeys) {
                            curr = curr.next;
                            idx=0;
                            // if next is null (last leaf in tree)
                            if (curr == null) {
                                return found;
                            }
                        }else{
                            idx ++;
                        }
                        val = curr.keys.get(idx);

                    }
                }


            } else {

                //TODO find leaf that is 12 or anything between or > 12
                var curr = getFirstNodeContaining(treeRoot,searchKey);

                System.out.println(curr.keys);

                if (curr.numKeys > 0) {

                    // getting the first val of node
                    int idx = 0;
                    T val = curr.keys.get(idx);

                    // while each value on leaf if < search key add to found

                    // get to the first gt val in the leaf and start there
                    while (IsALTB(val, searchKey)) {
                        val = curr.keys.get(idx);
                        System.out.println("---"+val);
                        if (idx+1 == curr.numKeys){
                            curr=curr.next;
                            idx = 0;
                            if (curr == null){
                                return found;
                            }
                        }else {
                            idx++;
                        }
                    }
                    System.out.println("+++"+val);

                    todo incriment to next val
                    while (IsAGTB(val, searchKey)) {
                        System.out.println("+++"+val);

                        //TODO add rp to found
                        System.out.print(val+"  ");



                        // if we are out of values for thus leaf go to next leaf
                        if (idx + 1 == curr.numKeys) {
                            curr = curr.next;
                            idx = 0;
                            // if next is null (last leaf in tree)
                            if (curr == null) {
                                return found;
                            }
                        }else{
                            idx++;
                        }
                        // get next value
                        val = curr.keys.get(idx);
                    }
                }

            }

            return found;

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("here in search range");
            return null;
        }
    }

    private BTreeNode<T> getFirstNodeContaining(BTreeNode<T> tree, T searchKey) {


        if (tree.isLeaf) {
            return tree;

        } else {
            var findIndex = 0;
            while (findIndex < tree.numKeys && IsALTB(tree.keys.get(findIndex), searchKey)) {
                findIndex++;
            }

            return this.getFirstNodeContaining(tree.children.get(findIndex), searchKey);
        }



    }


    public boolean insertElement(RecordPointer rp, T insertedValue) {


        if (this.treeRoot == null) {
            this.treeRoot = new BTreeNode<T>(this.nextIndex++, this.TreeNsize);
            this.treeRoot.keys.set(0, insertedValue);
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
            tree.keys.set(insertIndex, insertValue);


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
//            System.out.println("1 need to repair node " + tree.keys);

            this.treeRoot = this.split(tree);
            return true;
        } else {
//            System.out.println("2 need to repair node " + tree.keys);

            BTreeNode<T> newNode = this.split(tree);
            return this.insertRepair(newNode);
        }
    }


    public BTreeNode<T> split(BTreeNode<T> tree) {

        var rightNode = new BTreeNode<T>(this.nextIndex++, this.TreeNsize);

        T risingNode = tree.keys.get(this.split_index);
//        System.out.println("risingNode:  " + risingNode);

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

//        System.out.println("rightNode.numKeys " + rightNode.numKeys);


        //////// ---------  moving children for right node  --------- ////////

        for (i = rightSplit; i < tree.numKeys + 1; i++) {

            rightNode.children.set(i - rightSplit, tree.children.get(i));

            if (tree.children.get(i) != null) {
                rightNode.isLeaf = false;
                if (tree.children.get(i) != null) {
                    tree.children.get(i).parent = rightNode;
                }
                tree.children.set(i, null);
            }

        }
        // add keys to right node
        for (i = rightSplit; i < tree.numKeys; i++) {
            rightNode.keys.set(i - rightSplit, tree.keys.get(i));
        }


        //////// ---------  leftnode  --------- ////////

        BTreeNode<T> leftNode = tree;
        leftNode.numKeys = this.split_index;


        if (tree.parent != null) {

            return tree.parent;

            // the root becomes the rising node now
        } else {
            this.treeRoot = new BTreeNode<T>(this.nextIndex++, this.TreeNsize);
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



    public void print(BTreeNode<T> ROOT, String tab) {

        if (ROOT != null) {

            if (ROOT.isLeaf) {
                System.out.println(tab + "|--" + ROOT.keys.subList(0, ROOT.numKeys));

            } else {
                if (ROOT.parent != null) {
                    System.out.println(tab + "|-" + ROOT.keys.subList(0, ROOT.numKeys));

                } else {
                    System.out.println(tab + ROOT.keys.subList(0, ROOT.numKeys));

                }
                for (BTreeNode<T> child : ROOT.children) {


                    this.print(child, tab + "|  ");

                }
            }
        }
    }


    // delete
    public boolean deleteElement(RecordPointer rp, T deletedValue) {

        this.doDelete(this.treeRoot, deletedValue);
        if (this.treeRoot.numKeys == 0) {
            this.treeRoot = this.treeRoot.children.get(0);
            this.treeRoot.parent = null;
        }
        return true;
    }


    public boolean doDelete(BTreeNode<T> tree, T val) {
        if (tree != null) {

            var i = 0;
            for (i = 0; i < tree.numKeys && IsALTB(tree.keys.get(i), val); i++) ;

            if (i == tree.numKeys) {
                if (!tree.isLeaf) {

                    this.doDelete(tree.children.get(tree.numKeys), val);
                }

            } else if (!tree.isLeaf && tree.keys.get(i) == val) {

                this.doDelete(tree.children.get(i + 1), val);
            } else if (!tree.isLeaf) {

                this.doDelete(tree.children.get(i), val);
            } else if (tree.isLeaf && tree.keys.get(i) == val) {

                for (var j = i; j < tree.numKeys - 1; j++) {
                    tree.keys.set(j, tree.keys.get(j + 1));
                }
                tree.numKeys--;


                // Bit of a hack -- if we remove the smallest element in a leaf, then find the *next* smallest element
                //  (somewhat tricky if the leaf is now empty!), go up our parent stack, and fix index keys
                if (i == 0 && tree.parent != null) {

                    //????
                    T nextSmallest = null;
                    var parentNode = tree.parent;
                    var parentIndex = 0;
                    for (parentIndex = 0; parentNode.children.get(parentIndex) != tree; parentIndex++) ;

                    //???? next smallest
                    if (tree.numKeys == 0) {
                        if (parentIndex == parentNode.numKeys) {

                            //????
                            nextSmallest = null;
                        } else {
                            nextSmallest = parentNode.children.get(parentIndex + 1).keys.get(0);
                        }
                    } else {
                        nextSmallest = tree.keys.get(0);
                    }

                    while (parentNode != null) {
                        if (parentIndex > 0 && parentNode.keys.get(parentIndex - 1) == val) {
                            parentNode.keys.set(parentIndex - 1, nextSmallest);
                        }
                        var grandParent = parentNode.parent;
                        for (parentIndex = 0; grandParent != null && grandParent.children.get(parentIndex) != parentNode; parentIndex++)
                            ;
                        parentNode = grandParent;

                    }

                }
                this.repairAfterDelete(tree);

            }


        }
        return true;
    }


    public void repairAfterDelete(BTreeNode<T> tree) {

        // less then the min required
        if (tree.numKeys < this.min_keys) {

            // roor
            if (tree.parent == null) {
                // removing all keys in tree
                if (tree.numKeys == 0) {
                    this.treeRoot = tree.children.get(0);
                    //
                    if (this.treeRoot != null) {
                        this.treeRoot.parent = null;
                    }
                }

                // if not root
            } else {
                BTreeNode<T> parentNode = tree.parent;
                int parentIndex;
                for (parentIndex = 0; parentNode.children.get(parentIndex) != tree; parentIndex++) ;


                if (parentIndex > 0 && parentNode.children.get(parentIndex - 1).numKeys > this.min_keys) {
                    this.stealFromLeft(tree, parentIndex);

                } else if (parentIndex < parentNode.numKeys && parentNode.children.get(parentIndex + 1).numKeys > this.min_keys) {
                    this.stealFromRight(tree, parentIndex);

                } else if (parentIndex == 0) {
                    // Merge with right sibling
                    var nextNode = this.mergeRight(tree);
                    this.repairAfterDelete(nextNode.parent);
                } else {
                    // Merge with left sibling
                    var nextNode = this.mergeRight(parentNode.children.get(parentIndex - 1));
                    this.repairAfterDelete(nextNode.parent);

                }


            }
        }

    }


    public BTreeNode<T> mergeRight(BTreeNode<T> tree) {

        var parentNode = tree.parent;
        var parentIndex = 0;
        for (parentIndex = 0; parentNode.children.get(parentIndex) != tree; parentIndex++) ;
        var rightSib = parentNode.children.get(parentIndex + 1);

        if (!tree.isLeaf) {

            tree.keys.set(tree.numKeys, parentNode.keys.get(parentIndex));
        }


        var fromParentIndex = tree.numKeys;


        for (var i = 0; i < rightSib.numKeys; i++) {
            var insertIndex = tree.numKeys + 1 + i;
            if (tree.isLeaf) {
                insertIndex -= 1;
            }
            tree.keys.set(insertIndex, rightSib.keys.get(i));


        }


        if (!tree.isLeaf) {
            for (int i = 0; i <= rightSib.numKeys; i++) {
                tree.children.set(tree.numKeys + 1 + i, rightSib.children.get(i));
                tree.children.get(tree.numKeys + 1 + i).parent = tree;

            }
            tree.numKeys = tree.numKeys + rightSib.numKeys + 1;

        } else {
            tree.numKeys = tree.numKeys + rightSib.numKeys;

            tree.next = rightSib.next;

        }
        for (int i = parentIndex + 1; i < parentNode.numKeys; i++) {
            parentNode.children.set(i, parentNode.children.get(i + 1));

            parentNode.keys.set(i - 1, parentNode.keys.get(i));
        }
        parentNode.numKeys--;


        return tree;
    }


    public BTreeNode<T> stealFromRight(BTreeNode<T> tree, int parentIndex) {
        // Steal from right sibling
        var parentNode = tree.parent;


        var rightSib = parentNode.children.get(parentIndex + 1);
        tree.numKeys++;


        if (tree.isLeaf) {

            tree.keys.set(tree.numKeys - 1, rightSib.keys.get(0));
            parentNode.keys.set(parentIndex, rightSib.keys.get(1));

        } else {

            tree.keys.set(tree.numKeys - 1, parentNode.keys.get(parentIndex));
            parentNode.keys.set(parentIndex, rightSib.keys.get(0));
        }


        if (!tree.isLeaf) {
            tree.children.set(tree.numKeys, rightSib.children.get(0));
            tree.children.get(tree.numKeys).parent = tree;


            for (var i = 1; i < rightSib.numKeys + 1; i++) {
                rightSib.children.set(i - 1, rightSib.children.get(i));

            }

        }
        for (int i = 1; i < rightSib.numKeys; i++) {
            rightSib.keys.set(i - 1, rightSib.keys.get(i));
        }
        rightSib.numKeys--;

        return tree;

    }


    public BTreeNode<T> stealFromLeft(BTreeNode<T> tree, int parentIndex) {

        var parentNode = tree.parent;
        // Steal from left sibling
        tree.numKeys++;


        for (int i = tree.numKeys - 1; i > 0; i--) {
            tree.keys.set(i, tree.keys.get(i - 1));
        }
        var leftSib = parentNode.children.get(parentIndex - 1);


        if (tree.isLeaf) {

            tree.keys.set(0, leftSib.keys.get(leftSib.numKeys - 1));
            parentNode.keys.set(parentIndex - 1, leftSib.keys.get(leftSib.numKeys - 1));
        } else {

            tree.keys.set(0, parentNode.keys.get(parentIndex - 1));
            parentNode.keys.set(parentIndex - 1, leftSib.keys.get(leftSib.numKeys - 1));
        }


        if (!tree.isLeaf) {
            for (var i = tree.numKeys; i > 0; i--) {
                tree.children.set(i, tree.children.get(i - 1));

            }
            tree.children.set(0, leftSib.children.get(leftSib.numKeys));
            leftSib.children.set(leftSib.numKeys, null);
            tree.children.get(0).parent = tree;

        }
        leftSib.numKeys--;
        return tree;
    }


    public ArrayList<T> findElemen(T findValue) {

        return this.findInTree(this.treeRoot, findValue);

    }


    public ArrayList<T> findInTree(BTreeNode<T> tree, T searchVal) {


        //steps
        // search for leaf node where insert belongs
        // insert
        // repair
        if (tree.isLeaf) {


            ArrayList<RecordPointer> found = new ArrayList<>();
            ArrayList<T> foundKeys = new ArrayList<>();

            //12

            var curr = tree;

            if (curr != null) {

                int start = curr.keys.subList(0, curr.numKeys + 1).indexOf(searchVal);

                while (start != -1) {

                    for (T key : curr.keys.subList(0, curr.numKeys)) {
                        if (isEql(key, searchVal)) {
                            foundKeys.add(key);
                        }
                    }

                    curr = curr.next;
                    if (curr == null) {
                        return foundKeys;
                    }
                    start = curr.keys.subList(0, curr.numKeys + 1).indexOf(searchVal);
                }
            }

            return foundKeys;

        } else {
            var findIndex = 0;
            while (findIndex < tree.numKeys && IsALTB(tree.keys.get(findIndex), searchVal)) {
                findIndex++;
            }

            return this.findInTree(tree.children.get(findIndex), searchVal);
        }


    }


    private boolean IsAGTB(T key1, T key2) {
        return key1.compareTo(key2) > 0;
    }

    private boolean IsALTB(T key1, T key2) {
        return key1.compareTo(key2) < 0;
    }

    private boolean isEql(T key, T searchVal) {
        return key.compareTo(searchVal) == 0;
    }

}
