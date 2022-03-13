package filesystem;
import java.io.*;
import java.nio.file.*;

import catalog.Catalog;
import common.VerbosePrint;


/**
 * A static class for establishing and interaction with the Data Base's file system.
 *
 * Methods in this class are hard coded to adhere to the file system, which is why they are so
 * rigidly named (deletePageFile(page#) as opposed to deleteFile(file) - we need to know exactly where the file is,
 * and this reduces the chance of deleting files we don't own).
 *
 * FILE SYSTEM IS ESTABLISHED IN CATALOG.  We always base our file system on the db location the cat stores
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
    public static boolean setDBLocation(String dbLoc){
        if(hasLocation()){
            ERROR("Attempted to set database location when location already established.");
            return false;
        }
        location = dbLoc;
        return true;
    }

    /**
     * Getter for the database location.
     * Protects the use of the location variable, gives an error if location is not set
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
        TABS(DBLocation() + File.separator + "tabs"),
        PAGES(DBLocation() + File.separator + "pages"),
        CATALOG(DBLocation() + File.separator + "catalog");

        // code that allows for the enum to hold values
        // https://www.baeldung.com/java-enum-values
        public final String rel_loc;  //the location relative to the base location
        FilePath(String rel_loc) {
            this.rel_loc = rel_loc;
        }
    }

    public static boolean establishFileSystem(String location){
        if(!setDBLocation(location)){ // checks if File System already has a location, sets dblocation = location if not
            ERROR("File System not established, may already be managing a database.");
            return false;
        }

        Path pagePath = Paths.get(FilePath.PAGES.rel_loc);  // DBLocation should be set by first if
        Path tabsPath = Paths.get(FilePath.TABS.rel_loc);
        Path catPath = Paths.get(FilePath.CATALOG.rel_loc);


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

        // establish tabs directory
        if(!Files.exists(tabsPath)){ //if there's not already a tabs directory, make one
            try{
                Files.createDirectory(tabsPath);
            }
            catch(IOException e){  // shouldn't happen, but just in case
                ERROR("tabs directory did not exist, but errored on creation.");
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

        // if either the pages, tabs or catalog directories existed, they were left as they were
        return true;
    }


    // ---------------------------- PAGE MANAGEMENT -------------------------------------------
    // makes large use of  java.nio.file.Files: http://tutorials.jenkov.com/java-nio/files.html
    // would reccomend reading the docs
    /**
     * Deletes a page file using the established page path in
     * @return true - page file deleted / didn't exist anyway, false - page file not deleted (multiple reasons)
     */
    public static boolean deletePageFile(int num){
        if(!hasLocation()){
            ERROR("Cannot delete file, FileSystem not attached to location");
            return false;  // we don't even have a location!  we're not deleting anything D:<
        }

        String pageRelPath = String.format(File.separator+"%d", num);  // page's path relative to the pages directory
        Path pagePath = Paths.get(FilePath.PAGES.rel_loc+pageRelPath);

        try {
            Files.deleteIfExists(pagePath);
        }
        catch (IOException e){
            ERROR(String.format("Problem deleting page file: %d\n", num));
            ERROR(e.getMessage());
        }
        return true;
    }

    private static boolean writePageFile(){
        return false;
    }

    public static DataInputStream createPageDataInStream(String num) throws FileNotFoundException {

        String tabsLoc = FilePath.PAGES.rel_loc+File.separator+num;
        return createDataInStream(tabsLoc);
    }

    public static DataOutputStream createPageDataOutStream(String num) throws FileNotFoundException {
        String tabsLoc = FilePath.PAGES.rel_loc + File.separator + num;
        return createDataOutStream(tabsLoc);
    }

    // ---------------------------- CATALOG -----------------------------------------

    public static DataInputStream createCatDataInStream() throws FileNotFoundException {

        String catLoc = FilePath.CATALOG.rel_loc+File.separator+"catalog.txt";
        VerbosePrint.print("attempting to find catalog in: " + catLoc);
        return createDataInStream(catLoc);
    }

    public static DataInputStream createCatTabsDataInStream() throws FileNotFoundException {

        String tabsLoc = FilePath.TABS.rel_loc+File.separator+"tables.txt";
        return createDataInStream(tabsLoc);
    }

    public static DataOutputStream createCatDataOutStream() throws FileNotFoundException {
        String catLoc = FilePath.CATALOG.rel_loc+File.separator+"catalog.txt";
        return createDataOutStream(catLoc);
    }

    public static DataOutputStream createCatTabsDataOutStream() throws FileNotFoundException {
        String tabsLoc = FilePath.TABS.rel_loc+File.separator+"tables.txt";
        return createDataOutStream(tabsLoc);
    }


    // ------------------------------ UTILITY ---------------------------------------------

    /**
     * Util to create a data input stream.  Meant to be used in a static function where the path is hard-coded
     * @param path the path to which the data should be input from
     * @return a DataInputStream to the path
     * @throws FileNotFoundException
     */
    private static DataInputStream createDataInStream(String path) throws FileNotFoundException {
        try {
            // read in streams
            FileInputStream inputStream;
            inputStream = new FileInputStream(path);

            return new DataInputStream(inputStream);
        }
        catch (FileNotFoundException e){
            throw e;
        }
    }

    /**
     * Util to create a data output stream.  Meant to be used in a static function where the path is hard-coded
     * @param path the path to which the data should be output
     * @return a DataInputStream to the path
     * @throws FileNotFoundException
     */
    private static DataOutputStream createDataOutStream(String path) throws FileNotFoundException {
        try {
            return new DataOutputStream(
                    new BufferedOutputStream(
                            new FileOutputStream(path)));
        }
        catch (FileNotFoundException e){
            throw e;
        }
    }
    /**
     * Util function, so I don't have to type "System.err.println(FileSystem:" before every error message.
     * Made for printing out user errors such as trying to establish a FileSystem when one exists already.
     * @param error the user error to display
     */
    private static void ERROR(String error){
        System.err.println("FileSystem: " + error);
    }

    /**
     *
     * @param <X> element of type X
     * @param <Y> element of type Y
     */
    public class Tuple<X,Y>{
        public final X x;  //element 1
        public final Y y;  //element 2

        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }
}
