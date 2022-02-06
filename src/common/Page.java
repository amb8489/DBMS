package common;

public class Page {






    public boolean readFromDisk(String location) {
        System.err.println("ERROR: read from disk failed");
        return false;
    }

    public boolean writeToDisk() {
        System.err.println("ERROR: write to disk failed");
        return false;
    }
}
