package common;

public class Page {




    private String pageName;

    public String getPageName() {
        return pageName;
    }

    public boolean readFromDisk(String location) {
        System.err.println("ERROR: read from disk failed");
        return false;
    }

    public boolean writeToDisk() {
        System.err.println("ERROR: write to disk failed");
        return false;
    }
}
