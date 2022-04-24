package indexing;

import catalog.Catalog;
import common.Page;
import common.RecordPointer;
import common.Table;
import storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

//------------- TODO's ------------------


// ERROR IN DELETE WERE LAST NODE IS SAVE A DUP POINTER
//TODO comment up code

//TODO STORE/RESORE TO/FROM MEMORY--

//TODO put throws on all functions

//TODO give a table a b+ tree for its attributes, mk tree for pk always

// TODO optimization could add binary search to find values in nodes and not linear scan
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
    public String Type;


    // maps the start end end of pages that nodes


    public BPlusTree(int MaxPageSize, int MaxAttributeSize) {

        this.TreeNsize = (int) (Math.floor(MaxPageSize / ((double) (4 + MaxAttributeSize)) - 1));
        this.max_degree = TreeNsize;
        this.max_keys = TreeNsize - 1;
        this.min_keys = (int) Math.floor((TreeNsize + 1) / 2.0) - 1;
        this.split_index = (int) Math.floor((TreeNsize) / 2.0);

    }


    // for testing
    public BPlusTree(int degree) {
        this.TreeNsize = degree;
        this.max_degree = degree;
        this.max_keys = degree - 1;
        this.min_keys = (int) Math.floor((degree + 1) / 2.0) - 1;
        this.split_index = (int) Math.floor((degree) / 2.0);
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

        return this.findInTree(this.treeRoot, searchKey);

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
            if (lessThan) {


                var curr = getStartingLeaf(treeRoot);

                if (curr.numKeys > 0) {

                    // getting the first val of node
                    int idx = 0;
                    T val = curr.keys.get(idx);

                    // while each value on leaf if < search key add to found
                    // might not be true at first val in node but will be at some point in the leaf


                    while (IsALTB(val, searchKey, equalTo)) {

                        //add rp to found
                        found.add(curr.rps.get(idx));

                        // get next value


                        // if we are out of values for thus leaf go to next leaf
                        if (idx + 1 == curr.numKeys) {
                            curr = curr.next;
                            idx = 0;
                            // if next is null (last leaf in tree)
                            if (curr == null) {
                                return found;
                            }
                        } else {
                            idx++;
                        }
                        val = curr.keys.get(idx);

                    }
                }


            } else {

                var curr = getFirstNodeContaining(treeRoot, searchKey);


                if (curr.numKeys > 0) {

                    // getting the first val of node
                    int idx = 0;
                    T val = curr.keys.get(idx);

                    // while each value on leaf if < search key add to found

                    // get to the first gt val in the leaf and start there
                    while (IsALTB(val, searchKey, !equalTo)) {
                        if (idx + 1 == curr.numKeys) {
                            curr = curr.next;
                            idx = 0;
                            if (curr == null) {
                                return found;
                            }
                        } else {
                            idx++;
                        }
                        val = curr.keys.get(idx);

                    }


                    while (IsAGTB(val, searchKey, equalTo)) {

                        //add rp to found
                        found.add(curr.rps.get(idx));


                        // if we are out of values for thus leaf go to next leaf
                        if (idx + 1 == curr.numKeys) {
                            curr = curr.next;
                            idx = 0;
                            // if next is null (last leaf in tree)
                            if (curr == null) {
                                return found;
                            }
                        } else {
                            idx++;
                        }
                        // get next value
                        val = curr.keys.get(idx);
                    }
                }

            }

            return found;

        } catch (Exception e) {

            System.err.println("here in search range");
            return null;
        }
    }

    private BTreeNode<T> getFirstNodeContaining(BTreeNode<T> tree, T searchKey) {


        if (tree.isLeaf) {

//            int idxInNode = tree.keys.subList(0,tree.numKeys).indexOf(searchKey);
//            if (idxInNode == -1){
//                return  tree.next;
//            }

            return tree;

        } else {
            var findIndex = 0;
            while (findIndex < tree.numKeys && IsALTB(tree.keys.get(findIndex), searchKey, false)) {
                findIndex++;
            }

            return this.getFirstNodeContaining(tree.children.get(findIndex), searchKey);
        }


    }


    public boolean insertElement(RecordPointer rp, T insertedValue) {


        // first element in the tree


        if (this.treeRoot == null) {
            this.treeRoot = new BTreeNode<T>(this.nextIndex++, this.TreeNsize);
            this.treeRoot.keys.set(0, insertedValue);
            this.treeRoot.rps.set(0, rp);


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

            // find where to insert new value in leaf
            while (insertIndex > 0 && IsAGTB(tree.keys.get(insertIndex - 1), insertValue, false)) {
                tree.keys.set(insertIndex, tree.keys.get(insertIndex - 1));
                tree.rps.set(insertIndex, tree.rps.get(insertIndex - 1));
                insertIndex--;
            }

            // insert value
            tree.keys.set(insertIndex, insertValue);
            tree.rps.set(insertIndex, rp);

            // update the other record pointers that are > this record and on the same page
            int currRpIdx = insertIndex + 1;
            var currNode = tree;
            int updatePageName = rp.page();


            if (currRpIdx > currNode.numKeys - 1) {
                currRpIdx = 0;
                currNode = currNode.next;
                if (currNode == null) {

                    return this.insertRepair(tree);
                }
            }

            while (currNode.rps.get(currRpIdx).page() == updatePageName) {


                currNode.rps.set(currRpIdx, new RecordPointer(updatePageName, currNode.rps.get(currRpIdx).index() + 1));
                currRpIdx++;

                if (currRpIdx > currNode.numKeys - 1) {
                    currRpIdx = 0;


                    currNode = currNode.next;
                    if (currNode == null) {
                        return this.insertRepair(tree);
                    }
                }

            }

            // repairing tree
            return this.insertRepair(tree);

            // finding what child to move to
        } else {
            var findIndex = 0;
            while (findIndex < tree.numKeys && IsALTB(tree.keys.get(findIndex), insertValue, false)) {
                findIndex++;
            }

            return this.insert(tree.children.get(findIndex), insertValue, rp);
        }
    }

    public boolean insertRepair(BTreeNode<T> tree) {
        // all is okay no fix needed
        if (tree.numKeys <= this.max_keys) {

            return true;

            //root
        } else if (tree.parent == null) {

            this.treeRoot = this.split(tree);
            return true;
            // non root
        } else {

            BTreeNode<T> newNode = this.split(tree);

            // keep going till all is fixed
            return this.insertRepair(newNode);
        }
    }


    public BTreeNode<T> split(BTreeNode<T> tree) {

        // right split

        var rightNode = new BTreeNode<T>(this.nextIndex++, this.TreeNsize);

        // node that goes to the parent
        T risingNode = tree.keys.get(this.split_index);

        int i;
        int parentIndex = 0;

        // if not root
        if (tree.parent != null) {


            BTreeNode<T> currentParent = tree.parent;
            // finding what parent node to go to
            for (; parentIndex < currentParent.numKeys + 1 && currentParent.children.get(parentIndex) != tree; parentIndex++)
                ;


            // error checking
            if (parentIndex == currentParent.numKeys + 1) {
                throw new Error("Couldn't find which child we were!");
            }

            // adding the rising node to the parent, finding where to place it
            for (i = currentParent.numKeys; i > parentIndex; i--) {
                currentParent.children.set(i + 1, currentParent.children.get(i));
                currentParent.keys.set(i, currentParent.keys.get(i - 1));
            }
            // inserting into parent
            currentParent.numKeys++;
            currentParent.keys.set(parentIndex, risingNode);

            //updating
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


        //////// ---------  moving children for right node  --------- ////////

        for (i = rightSplit; i < tree.numKeys + 1; i++) {

            rightNode.children.set(i - rightSplit, tree.children.get(i));
            rightNode.rps.set(i - rightSplit, tree.rps.get(i));

            if (tree.children.get(i) != null) {
                rightNode.isLeaf = false;
                if (tree.children.get(i) != null) {
                    tree.children.get(i).parent = rightNode;
                }
                tree.children.set(i, null);
                tree.rps.set(i, null);

            }

        }
        // add keys to right node
        for (i = rightSplit; i < tree.numKeys; i++) {
            rightNode.keys.set(i - rightSplit, tree.keys.get(i));
            rightNode.rps.set(i - rightSplit, tree.rps.get(i));

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

    public void print() {
        print_h(this.treeRoot, " ");
    }

    public void print_h(BTreeNode<T> ROOT, String tab) {

        if (ROOT != null) {

            if (ROOT.isLeaf) {
                if (ROOT.parent == null) {
                    System.out.println("ROOT" + ROOT.keys.subList(0, ROOT.numKeys));

                } else {
                    // sromthing wtong with spitting ??
//                    System.out.println(tab + "|--" + ROOT.rps.subList(0, ROOT.numKeys));
                    System.out.println(tab + "|--" + ROOT.keys.subList(0, ROOT.numKeys));

                }
            } else {
                if (ROOT.parent != null) {

                    System.out.println(tab + "|-" + ROOT.keys.subList(0, ROOT.numKeys));

                } else {
                    System.out.println("ROOT" + ROOT.keys.subList(0, ROOT.numKeys));

                }
                for (BTreeNode<T> child : ROOT.children.subList(0, ROOT.numKeys + 1)) {


                    this.print_h(child, tab + "|  ");

                }
            }
        }
    }


    // delete
    public boolean deleteElement(RecordPointer rp, T deletedValue) {


        var success = this.doDelete(this.treeRoot, deletedValue, rp);
        if (this.treeRoot != null) {
            if (this.treeRoot.numKeys == 0) {

                this.treeRoot = this.treeRoot.children.get(0);
                if (this.treeRoot != null) {
                    this.treeRoot.parent = null;
                }
            }
        }
        return success;
    }


    public boolean doDelete(BTreeNode<T> tree, T val, RecordPointer rp) {
        if (tree != null) {

            var i = 0;
            for (i = 0; i < tree.numKeys && IsALTB(tree.keys.get(i), val, false); i++) ;


            if (i == tree.numKeys) {
                if (tree.isLeaf) {


                    boolean ValIsInLeaf = false;
                    for (int b = 0; b < tree.numKeys; b++) {
                        if (isEql(tree.keys.get(b), val)) {
                            ValIsInLeaf = true;
                            break;
                        }
                    }
                    if (!ValIsInLeaf) {
                        tree = tree.next;
                        i = tree.keys.indexOf(val);
                    }
                }
            }

            if (i == tree.numKeys) {
                if (!tree.isLeaf) {

                    this.doDelete(tree.children.get(tree.numKeys), val, rp);
                }

            } else if (!tree.isLeaf && tree.keys.get(i) == val) {
                this.doDelete(tree.children.get(i + 1), val, rp);

            } else if (!tree.isLeaf) {
                this.doDelete(tree.children.get(i), val, rp);
            } else if (tree.isLeaf && isEql(tree.keys.get(i), val)) {
/////
                ///////

                for (var j = i; j < tree.numKeys - 1; j++) {
                    tree.keys.set(j, tree.keys.get(j + 1));
                    tree.rps.set(j, tree.rps.get(j + 1));
                }
                tree.numKeys--;


                // Bit of a hack -- if we remove the smallest element in a leaf, then find the *next* smallest element
                //  (somewhat tricky if the leaf is now empty!), go up our parent stack, and fix index keys
                if (i == 0 && tree.parent != null) {

                    T nextSmallest;
                    RecordPointer nsRp;
                    var parentNode = tree.parent;
                    var parentIndex = 0;
                    for (parentIndex = 0; parentNode.children.get(parentIndex) != tree; parentIndex++) ;

                    // next smallest
                    if (tree.numKeys == 0) {

                        if (parentIndex == parentNode.numKeys) {
                            nextSmallest = null;
                            nsRp = null;

                        } else {

                            nextSmallest = parentNode.children.get(parentIndex + 1).keys.get(0);
                            nsRp = parentNode.children.get(parentIndex + 1).rps.get(0);
                        }


                    } else {

                        nextSmallest = tree.keys.get(0);
                        nsRp = tree.rps.get(0);

                    }

                    while (parentNode != null) {

                        if (parentIndex > 0 && parentNode.keys.get(parentIndex - 1) == val) {


                            parentNode.keys.set(parentIndex - 1, nextSmallest);

//
                            parentNode.rps.set(parentIndex - 1, nsRp);

                        }
                        var grandParent = parentNode.parent;
                        for (parentIndex = 0; grandParent != null && grandParent.children.get(parentIndex) != parentNode; parentIndex++)
                            ;
                        parentNode = grandParent;

                    }

                }

                var repair = tree;


                // ---- TODO somthing here is making the repair upset
//                //////-------updating the record pointers after a removal
//
//
                // update the other record pointers that are > this record and on the same page

                int deleteIdx = i;
                int currRpIdx = deleteIdx;
                var currNode = tree;

                int updatedPageName = rp.page();


                if (currRpIdx > currNode.numKeys - 1) {
                    currRpIdx = 0;
                    currNode = currNode.next;

                }
                if (currNode != null) {

                    while (currNode.rps.get(currRpIdx).page() == updatedPageName) {

                        currNode.rps.set(currRpIdx, new RecordPointer(updatedPageName, currNode.rps.get(currRpIdx).index() - 1));
                        currRpIdx++;

                        if (currRpIdx > currNode.numKeys - 1) {
                            currRpIdx = 0;


                            currNode = currNode.next;
                            if (currNode == null) {
                                break;
                            }
                        }

                    }
                }


                // ERROR COMING FROM HERE POSSIBLE TOO caused by shifting records above ??
                this.repairAfterDelete(tree);


            }


        }
        return false;
    }


    public void repairAfterDelete(BTreeNode<T> tree) {

        // less then the min required
        if (tree.numKeys < this.min_keys) {

            // root
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
            tree.rps.set(tree.numKeys, parentNode.rps.get(parentIndex));

        }


        var fromParentIndex = tree.numKeys;


        for (var i = 0; i < rightSib.numKeys; i++) {
            var insertIndex = tree.numKeys + 1 + i;
            if (tree.isLeaf) {
                insertIndex -= 1;
            }
            tree.keys.set(insertIndex, rightSib.keys.get(i));
            tree.rps.set(insertIndex, rightSib.rps.get(i));


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

        ///////------------------ parentNode.numKeys too big by 1
        for (int i = parentIndex + 1; i < parentNode.numKeys; i++) {

            parentNode.children.set(i, parentNode.children.get(i + 1));

            parentNode.keys.set(i - 1, parentNode.keys.get(i));
            parentNode.rps.set(i - 1, parentNode.rps.get(i));

        }
        parentNode.numKeys -= 1;


        return tree;
    }


    public BTreeNode<T> stealFromRight(BTreeNode<T> tree, int parentIndex) {
        // Steal from right sibling
        var parentNode = tree.parent;


        var rightSib = parentNode.children.get(parentIndex + 1);
        tree.numKeys++;


        if (tree.isLeaf) {

            tree.keys.set(tree.numKeys - 1, rightSib.keys.get(0));
            tree.rps.set(tree.numKeys - 1, rightSib.rps.get(0));

            parentNode.keys.set(parentIndex, rightSib.keys.get(1));
            parentNode.rps.set(parentIndex, rightSib.rps.get(1));


        } else {
            // todo--
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
            rightSib.rps.set(i - 1, rightSib.rps.get(i));

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
            tree.rps.set(i, tree.rps.get(i - 1));

        }
        var leftSib = parentNode.children.get(parentIndex - 1);


        if (tree.isLeaf) {

            tree.keys.set(0, leftSib.keys.get(leftSib.numKeys - 1));
            tree.rps.set(0, leftSib.rps.get(leftSib.numKeys - 1));

            parentNode.keys.set(parentIndex - 1, leftSib.keys.get(leftSib.numKeys - 1));
            parentNode.rps.set(parentIndex - 1, leftSib.rps.get(leftSib.numKeys - 1));

        } else {
            // todo--

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


    public ArrayList<RecordPointer> findInTree(BTreeNode<T> tree, T searchVal) {


        //steps
        // search for leaf node where insert belongs
        // insert
        // repair
        if (tree != null) {

            if (tree.isLeaf) {
                ArrayList<RecordPointer> found = new ArrayList<>();


                //12

                var curr = tree;


                int start = curr.keys.subList(0, curr.numKeys).indexOf(searchVal);

                if (start == -1) {
                    curr = curr.next;
                    if (curr == null) {
                        return found;
                    }
                    start = curr.keys.subList(0, curr.numKeys).indexOf(searchVal);
                }

                while (start != -1) {
                    int rpIdx = 0;
                    for (T key : curr.keys.subList(0, curr.numKeys)) {
                        if (isEql(key, searchVal)) {
                            found.add(curr.rps.get(rpIdx));
                        }
                        rpIdx++;
                    }

                    curr = curr.next;
                    if (curr == null) {
                        return found;
                    }
                    start = curr.keys.subList(0, curr.numKeys + 1).indexOf(searchVal);
                }

                return found;

            } else {
                var findIndex = 0;

                while (findIndex < tree.numKeys && IsALTB(tree.keys.get(findIndex), searchVal, true)) {
                    findIndex++;

                }

                return this.findInTree(tree.children.get(findIndex), searchVal);
            }
        }

        return new ArrayList<>();


    }


    private boolean IsAGTB(T key1, T key2, boolean equalTo) {

        if (equalTo) {
            return key1.compareTo(key2) > 0 || key1.compareTo(key2) == 0;
        }
        return key1.compareTo(key2) > 0;
    }

    private boolean IsALTB(T key1, T key2, boolean equalTo) {

        if (equalTo) {
            return key1.compareTo(key2) < 0 || key1.compareTo(key2) == 0;
        }
        return key1.compareTo(key2) < 0;
    }

    private boolean isEql(T key, T searchVal) {
        return key.compareTo(searchVal) == 0;
    }


    public static BPlusTree TreeFromTableAttribute(Table table, int AttributeIdx) {

        // check for out of bounds exception
        if (AttributeIdx >= table.getAttributes().size() || AttributeIdx < 0) {
            System.err.println(AttributeIdx + " out of bounds for size of " + table.getAttributes().size());
            return null;
        }


        // finding out what type of tree to make
        BPlusTree bpTree;


        String type = table.getAttributes().get(AttributeIdx).getAttributeType().toLowerCase();

        // gettting the init params for tree

        int maxPageSize = Catalog.getCatalog().getPageSize();
        int maxAttributeSize = table.getMaxAttributeSize();

        bpTree = switch (type.toLowerCase()) {
            case "integer" -> new BPlusTree<Integer>(maxPageSize, maxAttributeSize);
            case "double" -> new BPlusTree<Double>(maxPageSize, maxAttributeSize);
            case "boolean" -> new BPlusTree<Boolean>(maxPageSize, maxAttributeSize);
            default -> new BPlusTree<String>(maxPageSize, maxAttributeSize);
        };
        // set the type
        bpTree.Type = type;


        // making the tree

        return ((StorageManager) StorageManager.getStorageManager()).newIndex(table, bpTree, AttributeIdx);


    }


    public void writeToDisk() {
        writeToDisk_h(this.treeRoot);
    }

    private String writeToDisk_h(BTreeNode<T> ROOT) {

        if (ROOT != null) {


            if (ROOT.parent != null) {
                System.out.println(ROOT.writeOut());

            } else {
                System.out.println(ROOT.writeOut());

            }
            for (BTreeNode<T> child : ROOT.children.subList(0, ROOT.numKeys + 1)) {


                this.writeToDisk_h(child);

            }

        }
        return null;

    }

    public RecordPointer findInserPostion(T pkValue) {


        if (this.treeRoot == null) {
            // -1 means inset into that tables first page at index 0
            return new RecordPointer(-1, 0);
        } else {
            return findInserPostion_h(this.treeRoot, pkValue);
        }


    }

    private RecordPointer findInserPostion_h(BTreeNode<T> tree, T pkValue) {
        //steps
        // search for leaf node where insert belongs
        // insert
        // repair

        RecordPointer where = null;
        if (tree.isLeaf) {
            var insertIndex = tree.numKeys;

            // find where to insert new value in leaf
            while (insertIndex > 0 && IsAGTB(tree.keys.get(insertIndex - 1), pkValue, false)) {
                insertIndex--;
            }

            // get where it would inset
            //

            // carful for -1 idx or when  at the first element in leaf, that means
            // we want to insert before the first and not between

            RecordPointer prev = null;

            int onPageIndex = 0;

            // before the first element in the leaf
            if (insertIndex == 0) {
                prev = tree.rps.get(insertIndex);
                // first element and first index on page check
                if (prev.index() - 1 >= 0) {
                    onPageIndex = prev.index() - 1;
                }
            } else {
                prev = tree.rps.get(insertIndex - 1);
                onPageIndex = prev.index() + 1;

            }


            where = new RecordPointer(prev.page(), onPageIndex);

            return where;


        } else {
            var findIndex = 0;
            while (findIndex < tree.numKeys && IsALTB(tree.keys.get(findIndex), pkValue, false)) {
                findIndex++;
            }

            return this.findInserPostion_h(tree.children.get(findIndex), pkValue);
        }
    }


    public void updatePageNameAfterPageSplit(T startRecKey, int leftPageName, int newPageName, int numberRecsChanged, int numberRecsNotChanged) {


//

//        / find the first node contaings a record with page # old page

        var startingNode = getFirstNodeContaining(this.treeRoot, startRecKey);

        //  find idx of first occurrence in startRecSplit

        int idxInNode = startingNode.keys.subList(0, startingNode.numKeys).indexOf(startRecKey);

        if (idxInNode == -1) {
            startingNode = startingNode.next;
            idxInNode = startingNode.keys.subList(0, startingNode.numKeys).indexOf(startRecKey);
        }

        //. dups clause
        while (startingNode.rps.get(idxInNode).page() != leftPageName) {

            if (idxInNode >= startingNode.numKeys - 1) {
                idxInNode = 0;


                // go to next node weve looked though all the values for this node

                if (startingNode.next == null) {
                    return;
                }
                startingNode = startingNode.next;
            } else {
                idxInNode++;
            }
        }
        // startingNode.rps.get(idxInNode).index() should be  >= page.size - numberRecsChanged not 0 . dups clause
        //TODO < or <=
        // should start past the number of recs not being updated.. dups clause

        while (startingNode.rps.subList(0, startingNode.numKeys).get(idxInNode).index() < numberRecsNotChanged) {
            if (idxInNode >= startingNode.numKeys - 1) {
                idxInNode = 0;


                // go to next node weve looked though all the values for this node

                if (startingNode.next == null) {
                    return;
                }
                startingNode = startingNode.next;
            } else {
                idxInNode++;
            }

        }


        // the index in the new page
        int idxInSplitPage = 0;

        //current node we are updaing values for
        var curr = startingNode;
        // change the next n records to the new page

        while (idxInSplitPage < numberRecsChanged) {

            // maybe  idxInNode is not incrimenting on the first round
//            System.out.println("idxInNode:"+idxInNode + "  nodesize:"+curr.numKeys+" | idxpage:" + idxInSplitPage + "  page name:" + newPageName + " being changed " + curr.rps.get(idxInNode));
            curr.rps.set(idxInNode, new RecordPointer(newPageName, idxInSplitPage));


            if (idxInNode >= curr.numKeys - 1) {
                idxInNode = 0;


                // go to next node weve looked though all the values for this node

                if (curr.next == null) {

                    return;

                }
                curr = curr.next;


            } else {
                idxInNode++;
            }

            idxInSplitPage++;
        }


    }

    public void printRPS() {

        printRPS_h(this.treeRoot, " ");

    }

    public void printRPS_h(BTreeNode<T> ROOT, String tab) {

        if (ROOT != null) {

            if (ROOT.isLeaf) {
                if (ROOT.parent == null) {
                    System.out.println("ROOT" + ROOT.rps.subList(0, ROOT.numKeys));

                } else {
                    System.out.println(tab + "|--" + ROOT.rps.subList(0, ROOT.numKeys));
                }
            } else {
                if (ROOT.parent != null) {

                    System.out.println(tab + "|-" + ROOT.rps.subList(0, ROOT.numKeys));

                } else {
                    System.out.println("ROOT" + ROOT.rps.subList(0, ROOT.numKeys));

                }
                for (BTreeNode<T> child : ROOT.children.subList(0, ROOT.numKeys + 1)) {

                    this.printRPS_h(child, tab + "|  ");

                }
            }
        }
    }

    // everything != value
    public ArrayList<RecordPointer> searchNotEq(T value) {
        // everything less then
        var recs = searchRange(value, true, false);
        // and everything everything greater then
        recs.addAll(searchRange(value, false, false));
        return recs;
    }
}
