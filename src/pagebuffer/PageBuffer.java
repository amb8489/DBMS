/*

Aaron Berghash
*/

package pagebuffer;

import common.ITable;
import common.Page;
import common.Table;

import java.util.ArrayList;


// an in-memory buffer to store recently used pages
public class PageBuffer {


    /////////////////////////vars/////////////////////////////////////////////

    // max number of pages the buffer can hold before needing to write one out to disk
    private int maxBufferSize;

    // in order from least recently used to what was last used at the end
    // order enforced in loadNewPage()
    private static ArrayList<Page> pageBuffer = new ArrayList<>();


    ////////////////////////constructor///////////////////////////////////////
    public PageBuffer(int pageBufferSize) {
        this.maxBufferSize = pageBufferSize;
    }


    ///////////////////////methods////////////////////////////////////////////

    public static int getPageBufferSize() {
        return pageBuffer.size();
    }

    public Page getPageFromBuffer(String name, ITable table) {
        int idx = 0;

        // loooking to see if page we want is already loaded in the buffer
        for (Page p : pageBuffer) {
            if (name.endsWith(p.getPageName())) {


                // to update LRU order
                pageBuffer.add(pageBuffer.remove(idx));
                //gets the last element in list
                System.out.println("found page in buffer");
                return pageBuffer.get(pageBuffer.size() - 1);
            }
            idx++;
        }

        // if not found we need to load it in
        // loadNewPageToBuffer will place it in the buffer for us and auto make space and will place new page
        // where it should be in the array to keep LRU in order
        return loadNewPageToBuffer(name, table);
    }

    //write all pages in the buffer to disk and empty th e buffer
    public boolean PurgeBuffer() {

        //write all pages in buffer to disk
        for (Page page : pageBuffer) {

            if (page.isChanged()) {
                // check for successful page write
                if (!page.writeToDisk(page.getPageName(), page.IBelongTo)) {
                    System.err.println("error purging buffer [write to disk failed]");
                    return false;
                }
            }
        }
        // clear buffer
        pageBuffer.clear();
        return true;
    }


    public Page loadNewPageToBuffer(String name, ITable table) {
        System.out.println("buffer loading new page from disk");


        // if buffer is full
        if (pageBuffer.size() == maxBufferSize) {

            // write LRU page to disk / check for successful page write
            Page p = pageBuffer.get(0);
            System.out.println("BUFFER FULL wring page ["+p.getPageName()+"] to disk");

            //TODO ????/ "src/DB/pages/"+p.getPageName() hmmm
            if (!p.writeToDisk("src/DB/pages/"+p.getPageName(), p.IBelongTo)) {
                System.err.println("error loading new page to buffer [LRU write to disk failed]");
                return null;
            }
            // remove page from buffer
            pageBuffer.remove(0);
        }

        Page newPage = getPageFromDisk(name, (Table) table);
        pageBuffer.add(newPage);

        return newPage;
    }

    public Page getPageFromDisk(String name, Table table) {
        Page newPage = new Page(table);
        newPage.LoadFromDisk(name, table);
        return newPage;
    }


}
