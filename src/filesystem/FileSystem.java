package filesystem;
import java.io.IOException;
import java.nio.file.*;


/**
 * A static class for establishing and interaction with the Data Base's file system.
 *
 * Methods in this class are hard coded to adhere to the file system, which is why they are so
 * rigidly named (deletePageFile(page#) as opposed to deleteFile(file) - we need to know exactly where the file is,
 * and this reduces the chance of deleting files we don't own).
 *
 * Authors: Kyle Ferguson (krf6081@rit.edu)
 */
public class FileSystem {

    // ------------------------------- LOCATION OF THE DB ----------------------------------------------
    private static String location; //the base location for the DB.  must be established for any function to work

    private static boolean hasLocation(){
        return location != null;
    }

    /**
     * Provides the database location to the requester.
     * TODO - decide if this should be private or public.  Private for now because only FileSystem should use it
     * @return the database location
     */
    private static boolean setDBLocation(String dbLoc){
        if(hasLocation()){
            ERROR("Attempted to set database location when location already established.");
            return false;
        }
        location = dbLoc;
        return true;
    }

    /**
     * Getter for the database location.
     * TODO - decide if this isn't useful, or if it should be used internally
     * @return the database location
     */
    private static String DBLocation(){
        if (hasLocation())
            return location;
        ERROR("No location established.  Cannot interact with File System.");
        return null;
    }

    // ---------------------------- ESTABLISHING FILE SYSTEM -----------------------------------

    /**
     * Enum to hold relative paths to different directories and files.  Saves us from retyping the paths.
     *
     * How to add:
     * ENUM_NAME(String filepath_relative_to_db_location) - MUST HAVE BACKLASHES
     *
     * How to Access File Path:
     * FilePath.VALUE.rel_loc
     *  - will return whatever you put in as your string
     */
    private enum FilePath{
        PAGES("\\pages"),
        CATALOG("\\catalog");

        // code that allows for the enum to hold values
        // https://www.baeldung.com/java-enum-values
        public final String rel_loc;  //the location relative to the base location
        private FilePath(String rel_loc) {
            this.rel_loc = rel_loc;
        }
    }

    public static boolean establishFileSystem(String location){
        if(!setDBLocation(location)){ // checks if File System already has a location, sets dblocation = location if not
            ERROR("File System not established, may already be managing a database.");
            return false;
        }
        // check if pages file already exists
        Path pagePath = Paths.get(location+FilePath.PAGES.rel_loc);
        Path catPath = Paths.get(location+FilePath.CATALOG.rel_loc);

        // establish pages directory
        if(!Files.exists(pagePath)){ //if there's not already a pages directory, make one
            try{
                Files.createDirectory(pagePath);
            }
            catch(IOException e){  // shouldn't happen, but just in case
                ERROR("pages directory did not exist, but errored on creation.");
                ERROR(e.getMessage());
                return false;   //TODO decide if this should actually return false
            }
        }

        // establish catalog directory
        if(!Files.exists(catPath)){
            try{
                Files.createDirectory(catPath);
            }
            catch(IOException e){  // shouldn't happen, but just in case
                ERROR("catalog directory did not exist, but errored on creation.");
                ERROR(e.getMessage());
                return false;  //TODO ditto line 91
            }
        }

        // if either the pages or catalog directories existed, they were left as they were
        return true;
    }


    // ---------------------------- PAGE MANAGEMENT -------------------------------------------
    /**
     * Deletes a page file using the established page path in
     * @return
     */
    public static boolean deletePageFile(int num){
        if(!hasLocation()) return false;

        return false;
    }

    // ------------------------------ UTILITY ---------------------------------------------

    /**
     * Util function so I don't have to type "System.err.println(FileSystem:" before every error message.
     * Made for printing out user errors such as trying to establish a FileSystem when one exists already.
     * @param error the user error to display
     */
    private static void ERROR(String error){
        System.err.println("FileSystem: " + error);
    }
}
