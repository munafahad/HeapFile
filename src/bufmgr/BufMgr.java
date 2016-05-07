package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
	
	ArrayList<FrameDesc> buffer_pool;
	//int MAX;
	//pageToFrame: to map a disk page number to a frame descriptor 
	//+ to tell if the a disk page is not in the buffer pool
	HashMap<PageId, FrameDesc> pageFrameMap;
	Clock replPolicy;
	
  public BufMgr(int numframes) {

	  //MAX = numframes;
	  
	  buffer_pool = new ArrayList<FrameDesc>();
	  
	  for (int i = 0; i < numframes; ++i) {
		  buffer_pool.add(new FrameDesc());
	  }

	  pageFrameMap = new HashMap<PageId, FrameDesc>();
	  replPolicy = new Clock();
	  
  } // public BufMgr(int numframes)

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

	//frameNo: to get the frame that holds the page if it is exist in the buffer pool 
	  FrameDesc frameNo = pageFrameMap.get(pageno);
		  
	// If disk page pageno is already in the buffer pool ==> increment pin_count of that frame.  
	  if(frameNo != null) {
          frameNo.pin_count++;

      } else {
		  FrameDesc victimFrm;
		  //uses the "Clock" replacement policy to select a frame to replace
		  if (!buffer_pool.isEmpty()) {
			  victimFrm = buffer_pool.get(buffer_pool.size()-1);
			  buffer_pool.remove(victimFrm);
		  } else {
			  victimFrm = replPolicy.pickVictim(this);

			  //if we couldn't find an available frame, so return an error
			  if (victimFrm == null) {
				  throw new IllegalStateException("All pages are pinned (pool is full)!");
			  } else {
				  //if frame to use is dirty ==> flush page to disk 
				  if (victimFrm.dirty) {
					  flushPage(pageno, victimFrm);
				  }
			  }

			  //remove previous page from the frame if any
			  pageFrameMap.remove(pageno);

			  //reset the frame details
			  victimFrm.pin_count = 0;
			  victimFrm.valid = false;
			  victimFrm.dirty = false;
			  victimFrm.refbit = false;
		  }

			  
			  switch (contents)
			  {
			  case PIN_DISKIO: {
				 //read the page from disk into the frame 
				 
		            
		          Minibase.DiskManager.read_page(pageno, victimFrm);
		    
		          victimFrm.pin_count ++;
		          victimFrm.valid = true;
		          victimFrm.dirty = false;
				  victimFrm.pageno = new PageId();
		          victimFrm.pageno.copyPageId(pageno);
		          victimFrm.refbit = true;
		          
		          pageFrameMap.put(victimFrm.pageno, victimFrm);
		          mempage.setData(victimFrm.getData());
				
	              
				  break;
			  }
			  case PIN_MEMCPY: {
				//copy mempage into the frame
				  
				  victimFrm.pin_count++;
		          victimFrm.valid = true;
		          victimFrm.dirty = false;
				  victimFrm.pageno = new PageId();
		          victimFrm.pageno.copyPageId(pageno);
		          victimFrm.refbit = true;
	              

		          pageFrameMap.put(victimFrm.pageno, victimFrm);
		          victimFrm.setPage(mempage);
		           
				  break;
			  }
			  case PIN_NOOP: {
				//copy nothing into the frame - the frame contents are irrelevant

                  victimFrm.pin_count++;
                  victimFrm.valid = true;
                  victimFrm.dirty = false;
                  victimFrm.pageno = new PageId(pageno.pid);
                  victimFrm.refbit = true;

                  pageFrameMap.put(pageno, victimFrm);
                  //victimFrm.setPage(mempage);

                  //set page and data
//                  mempage.setPage(victimFrm);
//                  mempage.setData(victimFrm.getData());
//	              
				  break;
			  }
			  }
		  }
	  
  } // public void pinPage(PageId pageno, Page page, int contents)
  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

		  //frameNo: to get the frame that holds the page if it is exist in the buffer pool 
		  FrameDesc frameNo = pageFrameMap.get(pageno);
		  
		  //first check if the page is not in the buffer pool OR not pinned
		  if(frameNo == null || frameNo.pin_count == 0)
			{
			  throw new IllegalArgumentException("Page is not in the buffer pool or not pinned!"+ "P###"
					  + pageno.toString() + ":" + pageno.pid);
			}
			else
			{
				//is page updated or not? 
				//update dirty "field" according to that 
				if(dirty)
					frameNo.dirty = UNPIN_DIRTY; //UNPIN_DIRTY = true ==> write update to disk
				else
					frameNo.dirty = UNPIN_CLEAN; //UNPIN_CLEAN = false ==> no update (no need to write back to disk)
				
				//decrease pin_count variable for that frame
				frameNo.pin_count--;
				
				//set "refbit" to true when pin_count is set to 0
				if (frameNo.pin_count == 0)
			      {
			        frameNo.refbit = true;
			      }
			}
  } // public void unpinPage(PageId pageno, boolean dirty)
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page firstpg, int run_size) {

	  //pageno: "Page Id" of 1st allocated page
	  PageId pageno = new PageId();

	  //frameNo: is used to check if firstpg is already pinned
	  FrameDesc frameNo = pageFrameMap.get(pageno);

	  //1. check if there is a free frame in buffer pool
	  if(getNumUnpinned() == 0) {
		  throw new IllegalStateException("All pages are pinned!");
	  }
	  //2. check if first page is allocated or not
	  //also check if the page is pinned or not
	  else if (frameNo != null && frameNo.pin_count > 0) {
		  throw new IllegalArgumentException("firstpg is already pinned!");
	  }
	  //3. otherwise; "allocate" and "pinPage" 
	  else {
		  pageno.pid = Minibase.DiskManager.allocate_page(run_size).pid;
		  pinPage(pageno, firstpg, PIN_MEMCPY);
	  }
	  
	  //return "Page Id" of 1st allocated page
	  return pageno;

  } // public PageId newPage(Page firstpg, int run_size)

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {

		  
		//frameNo: is used to check if firstpg is already pinned
		FrameDesc frameNo = pageFrameMap.get(pageno);
	    
		//1. check if pageno in the buffer pool already
		// and if it is pinned or not
		if(frameNo != null && frameNo.pin_count > 0) {
			throw new IllegalArgumentException("The page is pinned!");
		}
		//2. otherwise; deallocate the page / free it from the pool
		else {
			if(pageFrameMap.containsKey(pageno.pid)) {
				pageFrameMap.remove(pageno);
			}
			Minibase.DiskManager.deallocate_page(pageno);
			
		}

  } // public void freePage(PageId firstid)

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllFrames() {

	  Iterator map = pageFrameMap.entrySet().iterator();
	  while (map.hasNext()) {
		  Map.Entry pair = (Map.Entry) map.next();
		  PageId key = (PageId) pair.getKey();
		  FrameDesc value = (FrameDesc) pair.getValue();
		  map.remove();
		  if (value.valid && value.dirty) {
			  flushPage(key, value);
		  }
	  }
	  } // public void flushAllFrames()

  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno, FrameDesc frameNo) {
	 
		if(frameNo != null)
		{
			if (frameNo.dirty) {
				//write page to disk
				Minibase.DiskManager.write_page(pageno, frameNo);
			}
		}
		else
		{
			 throw new IllegalArgumentException("Page is not in the buffer pool!");
		}
	
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumFrames() {
    
	  return pageFrameMap.size();

  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {

      int unpinned = 0;

      if (!buffer_pool.isEmpty()) {
          unpinned += buffer_pool.size();
      }

      Iterator map = pageFrameMap.entrySet().iterator();
      while (map.hasNext()) {
          Map.Entry pair = (Map.Entry) map.next();
          PageId key = (PageId) pair.getKey();
          FrameDesc value = (FrameDesc) pair.getValue();
          if (value.pin_count == 0) {
              unpinned++;
          }
      }

      return unpinned;
  }

} // public class BufMgr implements GlobalConst
