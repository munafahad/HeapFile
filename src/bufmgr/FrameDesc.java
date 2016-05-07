package bufmgr;

import global.Page;
import global.PageId;

public class FrameDesc extends Page {

	/*
		* Each frame has certain states associated with it. These states include whether the frame
		is dirty, whether it includes valid data (data which reflects data in a disk page), and if it
		includes valid data then what is the disk page number of the data, how many callers
		have pins on the data (the pin count), and any other informa>on you wish to store, for
		example informa>on relevant to the replacement algorithm. Be sure to store this informa
		>on as efficiently as possible while preserving readability
	 */
	
	protected boolean dirty;
	protected boolean valid; //valid data in frame or not
	protected PageId pageno; //disk page number
	protected int pin_count; // # of callers who pin data
	protected boolean refbit;
	
	public FrameDesc() {
		dirty = false;
		valid = false; 
		pageno = null; //OR new PageId();
		pin_count = 0;
		refbit = false;
		
	}

}