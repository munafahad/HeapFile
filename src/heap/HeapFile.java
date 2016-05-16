package heap; 

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is the simplest database file structure.  It is an unordered 
 * set of records, stored on a set of data pages. <br>
 * This class supports inserting, selecting, updating, and deleting
 * records.<br>
 * Normally each heap file has an entry in the database's file library.
 * Temporary heap files are used for external sorting and in other
 * relational operators. A temporary heap file does not have an entry in the
 * file library and is deleted when there are no more references to it. <br>
 * A sequential scan of a heap file (via the HeapScan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  /** HFPage type for directory pages. */
  protected static final short DIR_PAGE = 10;

  /** HFPage type for data pages. */
  protected static final short DATA_PAGE = 11;

  // --------------------------------------------------------------------------

  /** Is this a temporary heap file, meaning it has no entry in the library? */
  protected boolean isTemp;

  /** The heap file name.  Null if a temp file, otherwise 
   * used for the file library entry. 
   */
  protected String fileName;

  /** First page of the directory for this heap file. */
  protected PageId headId;
  
  //private HFPage hfPage;
  //private DirPage dirPage;
  //private int recCount;

  // --------------------------------------------------------------------------

  /**
   * If the given name is in the library, this opens the corresponding
   * heapfile; otherwise, this creates a new empty heapfile. 
   * A null name produces a temporary file which
   * requires no file library entry.
   */
  public HeapFile(String name) {

	  this.fileName=name;
	  
	  //1. chack file name
	  if(name!=null){
		  
		  //2. if the file has a name ==> it has (or will have) an entry in the library
		  isTemp = false;
		  headId = Minibase.DiskManager.get_file_entry(name);
		  
		  if(headId == null){
			
				 DirPage dirPage = new DirPage(); //DirPage extends HFPage
				 headId = Minibase.BufferManager.newPage(dirPage, 1);
				 dirPage.setCurPage(headId);
				 Minibase.DiskManager.add_file_entry(name, headId);	
				 
				 Minibase.BufferManager.unpinPage(dirPage.getCurPage(), UNPIN_DIRTY); 
				  
		  } 
		  //the file do not have an entry in the library
		  else {
			  isTemp = true;
			  }
	  } else {
		  throw new IllegalArgumentException("FILE NAME CANNOT BE NULL!");
	  }


	  }// public HeapFile(String name)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {

	  //if(!isTemp){
	  if(isTemp){
		  deleteFile();
	  }
  } // protected void finalize() throws Throwable

  /**
   * Deletes the heap file from the database, freeing all of its pages
   * and its library entry if appropriate.
   */
  public void deleteFile() {

	  //1. free all data pages and dir pages if any
	  PageId dirId = new PageId(headId.pid);
      DirPage dirPage = new DirPage();
      PageId nextId;
      
      while(dirId.pid != INVALID_PAGEID) {
    	  
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          
          //1. free data pages
          int count = dirPage.getEntryCnt();
          for(int i = 0; i < count; i++)
          {
        	  //Minibase.BufferManager.unpinPage(dirPage.getPageId(i), UNPIN_CLEAN);
        	  Minibase.BufferManager.freePage(dirPage.getPageId(i));
          }

          //free dir pages
          nextId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          Minibase.BufferManager.freePage(dirId);
          
          
          dirId = nextId;
      }

      //2. delete the file from the library
      if(!isTemp)
          Minibase.DiskManager.delete_file_entry(fileName);
     
  } // public void deleteFile()

  /**
   * Inserts a new record into the file and returns its RID.
   * Should be efficient about finding space for the record.
   * However, fixed length records inserted into an empty file
   * should be inserted sequentially.
   * Should create a new directory and/or data page only if
   * necessary.
   * 
   * @throws IllegalArgumentException if the record is too 
   * large to fit on one data page
   */
  public RID insertRecord(byte[] record) {

	  
	  if(record.length > MAX_TUPSIZE) {
          throw new IllegalArgumentException("the record is too large to fit on one data page");
      } else {
    	  //HFPage.java: int spaceNeeded = recLength + SLOT_SIZE;
    	  
    	  //1. find a page that has a free space
    	  PageId pageId = getAvailPage(record.length + HFPage.getSlotSize());
          //System.out.println(pageId.pid);
    	  DataPage dataPage = new DataPage();
    	  
          //2. insert record to the "data page"
          Minibase.BufferManager.pinPage(pageId, dataPage, PIN_DISKIO);
          RID rid = dataPage.insertRecord(record); 
          //dataPage.print();
          
          //3. update file/page directory
          short freeSpace = dataPage.getFreeSpace();
          updateDirEntry(pageId, 1, freeSpace); //1 as we need to add a new record
          
          //4. unpin the used pages "dir page" and "data page"
          Minibase.BufferManager.unpinPage(pageId, UNPIN_DIRTY);
          //Minibase.BufferManager.unpinPage(dataPage.getNextPage(), UNPIN_CLEAN);
          //Minibase.BufferManager.unpinPage(dataPage.getPrevPage(), UNPIN_CLEAN);
          // Minibase.BufferManager.unpinPage(rid.pageno,UNPIN_CLEAN);
          return rid; 
          
      }
	
   } // public RID insertRecord(byte[] record)

  /**
   * Reads a record from the file, given its rid.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) {

	  byte [] rec;
	  DataPage page = new DataPage();
	  
      Minibase.BufferManager.pinPage(rid.pageno, page, PIN_DISKIO);
      
      try {
          rec = page.selectRecord(rid);
      } catch(IllegalArgumentException exc) {
          throw exc;
      }
      
      
      Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
      return rec;
     
  } // public byte[] selectRecord(RID rid)

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) {

	  DataPage dataPage = new DataPage();
      Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);
      
      try {
    	  dataPage.updateRecord(rid, newRecord);
          Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
      } catch(IllegalArgumentException exc) {
          Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
          throw exc;
      }
      
  } // public void updateRecord(RID rid, byte[] newRecord)

  /**
   * Deletes the specified record from the heap file.
   * Removes empty data and/or directory pages.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) {

	  DataPage dataPage = new DataPage();
	  
      Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);
      
      try {
    	  //1.delete from the dataPage
    	  dataPage.deleteRecord(rid);
    	  
    	  //2. update the free space in directory page
          short freeCount = dataPage.getFreeSpace(); 
          updateDirEntry(rid.pageno, -1, freeCount);
          
          Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
      } catch(IllegalArgumentException exc) {
          Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
          throw exc;
      }
      
      //Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
     
  } // public void deleteRecord(RID rid)

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {

	  int recCount = 0;
      
      PageId dirId = new PageId(headId.pid);
      DirPage dirPage = new DirPage();
      
      PageId nextId;
      
      while(dirId.pid != INVALID_PAGEID)
      {
          
    	  Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
    	  
    	  //count records in a single page
    	  //to get number of "directory entries" on the page
    	  int count = dirPage.getEntryCnt();
    	  
          for(int i = 0 ; i < count; i++) {
        	  //to getRecordsCount at a given index
        	  //System.out.println(recCount);
        	  recCount += dirPage.getRecCnt(i); 
          }
         
          //go to next page
          nextId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          dirId = nextId;
      }

     
      return recCount;
  
  } // public int getRecCnt()

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Searches the directory for the first data page with enough free space to store a
   * record of the given size. If no suitable page is found, this creates a new
   * data page.
   * A more efficient implementation would start with a directory page that is in the
   * buffer pool.
   */
  protected PageId getAvailPage(int reclen) {
   
      PageId freeId = null;
      PageId dirId = new PageId(headId.pid);
      DirPage dirPage = new DirPage();
      PageId nextId;
      
      while(freeId == null && dirId.pid != INVALID_PAGEID)
      {
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          
          int count = dirPage.getEntryCnt();
          for(int i = 0; i < count; i++)
          {
              if(dirPage.getFreeCnt(i) < reclen + HFPage.getSlotSize())
                  continue;
              
              freeId = dirPage.getPageId(i);
              
              break;
          }

          nextId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          dirId = nextId;
      }

     //create a new Data Page if no free space on any existing page
      if(freeId == null)
          freeId = insertPage();
      return freeId;

  } // protected PageId getAvailPage(int reclen)

  /**
   * Helper method for finding directory entries of data pages.
   * A more efficient implementation would start with a directory
   * page that is in the buffer pool.
   * 
   * @param pageno identifies the page for which to find an entry
   * @param dirId output param to hold the directory page's id (pinned)
   * @param dirPage output param to hold directory page contents
   * @return index of the data page's entry on the directory page
   */
  protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage) {

	  //int entry = -1;
	  PageId nextId;
	
	  //int loop_times = 0 ;
	  
	  //1. check one page directory at a time  
	  for(dirId.pid = headId.pid ; dirId.pid != INVALID_PAGEID ; dirId.pid = nextId.pid){
			
		  
		  		 //System.out.println(dirId.pid);
			  Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
			   
			  //2. get entry count in a dirPage
	          int count = dirPage.getEntryCnt();
	          
	          //3. check if there is a pointer to a dataPage
	          for(int i = 0; i < count ; i++){
	              if(pageno.pid == dirPage.getPageId(i).pid) {
	            	  //System.out.println(dirPage.getPageId(i).pid);
	            	  //Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
	            	  return i;}
	          }
	          
	         
	          nextId = dirPage.getNextPage();
	          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
	         
		  } 
	  
	  
       return -1;  
 
	
  } // protected int findEntry(PageId pageno, PageId dirId, DirPage dirPage)

  /**
   * Updates the directory entry for the given data page.
   * If the data page becomes empty, remove it.
   * If this causes a dir page to become empty, remove it
   * @param pageno identifies the data page whose directory entry will be updated
   * @param deltaRec input change in number of records on that data page
   * @param freecnt input new value of freecnt for the directory entry
   */
  protected void updateDirEntry(PageId pageno, int deltaRec, int freecnt) {
	  
	  PageId dirId = new PageId(); //output parameter 
      DirPage dirPage = new DirPage();
      
      int index = findDirEntry(pageno, dirId, dirPage);
      
      //Minibase.BufferManager.pinPage(dirId, dirPage,PIN_DISKIO);
      
      //check if the page is empty 
      int recCount = dirPage.getRecCnt(index) + deltaRec;
      if(recCount < 1)
      {
    	  //Minibase.BufferManager.unpinPage(pageno, UNPIN_DIRTY);
    	  //delete the page if it is empty
          deletePage(pageno, dirId, dirPage, index);
      } else {
    	  //update page metadata in dirPage
          dirPage.setRecCnt(index, (short)recCount);
          dirPage.setFreeCnt(index, (short)freecnt);
          
          Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
      }
	  
  } // protected void updateEntry(PageId pageno, int deltaRec, int deltaFree)

  /**
   * Inserts a new empty data page and its directory entry into the heap file. 
   * If necessary, this also inserts a new directory page.
   * Leaves all data and directory pages unpinned
   * 
   * @return id of the new data page
   */
  protected PageId insertPage() {
	  
	  //PART 1: to manage the dirPage
	  int count = 0;
	  
	  DirPage dirPage = new DirPage();
      PageId dirId = new PageId(headId.pid);
      
      do
      {
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          
          count = dirPage.getEntryCnt();
          
          //break if there is a free space in current page
          //if(count < dirPage.getMaxEntries()) {
          if(count < 125) {
        	  break;
          } else {
        	//new page
              PageId nextId = dirPage.getNextPage();
              
              
              if(nextId.pid == INVALID_PAGEID) {
                  DirPage newDirPage = new DirPage();
                  PageId newDirId = Minibase.BufferManager.newPage(newDirPage, 1);
                  //Minibase.BufferManager.unpinPage(newDirId, UNPIN_CLEAN);
                  
                  newDirPage.setCurPage(newDirId);
                  
                  //not sure about this step
                  dirPage.setNextPage(newDirId);
                  newDirPage.setPrevPage(dirId);
                  
                  Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
                  
                  dirId = newDirId;
                  dirPage = newDirPage;
                  count = 0;
                  
                 //Minibase.BufferManager.unpinPage(newDirId, UNPIN_CLEAN);
                  
                  break;
              }
              
              Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
              
              dirId = nextId;
             
          }
        	  
          
          
          
      } while(true);
      
      
      //PART 2: manage the data page
      DataPage dataPage = new DataPage();
      PageId dataId = Minibase.BufferManager.newPage(dataPage, 1);
      
      dataPage.setCurPage(dataId);
      
      //PART3: update dir page
      dirPage.setPageId(count, dataId);
      dirPage.setRecCnt(count, (short)0);
      dirPage.setFreeCnt(count, dataPage.getFreeSpace());
      dirPage.setEntryCnt((short)(++count));
      
      //Minibase.BufferManager.unpinPage(newDirPage., UNPIN_DIRTY);
      //Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
      Minibase.BufferManager.unpinPage(dataId, UNPIN_DIRTY);
      Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
      
      return dataId;
      
  } // protected PageId insertPage()

  /**
   * Deletes the given data page and its directory entry from the heap file. If
   * appropriate, this also deletes the directory page.
   * 
   * @param pageno identifies the page to be deleted
   * @param dirId input param id of the directory page holding the data page's entry
   * @param dirPage input param to hold directory page contents
   * @param index input the data page's entry on the directory page
   */
  protected void deletePage(PageId pageno, PageId dirId, DirPage dirPage,
      int index) {

	  //1. free and unpin the page
	  Minibase.BufferManager.freePage(pageno);
	  Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
	  
	  //2. compact dir page
      dirPage.compact(index);
      
      
      //3. do needed changes in dirPage
      short entryCount = dirPage.getEntryCnt();
      
      //in case of not a head page; we can free the dir page and set previous and next dirPages
      if(dirId.pid != headId.pid && entryCount == 1)
      {
          DirPage dPage = new DirPage();
          PageId prevId = dirPage.getPrevPage();
          PageId nextId = dirPage.getNextPage();
          
          //previous page
          if(prevId.pid != INVALID_PAGEID){
              Minibase.BufferManager.pinPage(prevId, dPage, PIN_DISKIO);
              dPage.setNextPage(nextId);
              Minibase.BufferManager.unpinPage(prevId, UNPIN_DIRTY);
          }
          
          
          //next page
          if(nextId.pid != INVALID_PAGEID){
              Minibase.BufferManager.pinPage(nextId, dPage, PIN_DISKIO);
              dPage.setPrevPage(prevId);
              Minibase.BufferManager.unpinPage(nextId, UNPIN_DIRTY);
          }
          
          
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          Minibase.BufferManager.freePage(dirId);
      } 
      
      //do not delete the directory file if it is the head page
      else {
          dirPage.setEntryCnt(--entryCount);
          Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
      }
      
  } // protected void deletePage(PageId, PageId, DirPage, int)

} // public class HeapFile implements GlobalConst
