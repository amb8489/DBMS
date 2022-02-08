/*

Aaron Berghash
*/


package pagebuffer;

import common.Page;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Set;


// an in-memory buffer to store recently used pages
public class PageBuffer {


    /////////////////////////vars/////////////////////////////////////////////

    // max number of pages the buffer can hold before needing to write one out to disk
    private int maxBufferSize;

    // in order from least recently used to what was last used at the end
    // order enforced in loadNewPage()
    private static ArrayList<Page>pageBuffer = new ArrayList<>();




    ////////////////////////constructor///////////////////////////////////////
    public PageBuffer(int pageBufferSize){
        this.maxBufferSize = pageBufferSize;
    }








    ///////////////////////methods////////////////////////////////////////////

    public Page getPageFromBuffer(String name){
        int idx = 0;
        for (Page p:pageBuffer){
            if (p.getPageName().equals(name)){
                pageBuffer.add(pageBuffer.remove(idx));
                return pageBuffer.get(pageBuffer.size());
            }
            idx++;
        }
        return getPageFromBuffer(name);
    }

    //write all pages in the buffer to disk and empty th e buffer
    public boolean PurgeBuffer(){

        //write all pages in buffer to disk
        for(Page page: pageBuffer){
            // check for successful page write
            if(! page.writeToDisk()){
                System.err.println("error purging buffer [write to disk failed]");
                return false;
            }
        }
        // clear buffer
        pageBuffer.clear();
        return true;
    }



    public boolean loadNewPageToBuffer(String name){

        // if buffer is full
        if (pageBuffer.size() == maxBufferSize){

            // write LRU page to disk / check for successful page write
            if(! pageBuffer.get(0).writeToDisk()){
                System.err.println("error loading new page to buffer [LRU write to disk failed]");
                return false;
            }
            // remove page from buffer
            pageBuffer.remove(0);
        }

        Page newPage = getPageFromDisk(name);
        pageBuffer.add(newPage);

        return true;
    }

    // TODO load requested page
    public Page getPageFromDisk(String name){


        

        return null;
    }






}
