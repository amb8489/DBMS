package filesystem;

public class FSTest {
    public static void main(String[] args) {
        String location = "DB";  //YOUR LOCATION HERE
        FileSystem.establishFileSystem(location);

        FileSystem.deletePageFile(1);
        FileSystem.deletePageFile(2);
        FileSystem.deletePageFile(3);
        FileSystem.deletePageFile(4);
        FileSystem.deletePageFile(5);
        FileSystem.deletePageFile(6);
    }
}
