package common;

import catalog.Catalog;
import database.Database;
import storagemanager.StorageManager;

import java.util.Scanner;

public class Phase3TestSelect {


    public static void main(String[] args) {

        // Startup time!  Establish a catalog and a storage manager for us to work with
        Catalog.createCatalog("DB",4048 ,3);
        StorageManager.createStorageManager();
        String statement = "select";
        Database.executeQuery(statement);
    }
}
