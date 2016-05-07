package bufmgr;

import global.GlobalConst;
import global.PageId;

import java.util.*;


public class Clock implements GlobalConst {

	protected List<FrameDesc> pageFrameArray;
	protected int current;
	 
	public Clock() {

		this.pageFrameArray = null;
		this.current = 0;
	}



    public FrameDesc pickVictim(BufMgr bm) {

        this.pageFrameArray = new ArrayList<FrameDesc>(bm.pageFrameMap.values());

        //((pageFrameArray.size() * 2): as we need to check the buff. pool 2 times
        for (int i = 0; i < (pageFrameArray.size() * 2); i++) {
            //current = i % pageFrameArray.size();
            //1. if data in bufpool[current] is not valid, choose current
            if (pageFrameArray.get(current).valid != true) {
                return pageFrameArray.get(current);
            }
            //2. if frametab[current]'s pin count is 0
            else {
                if (pageFrameArray.get(current).pin_count == 0) {
                    // check if frametab [current] has refbit
                    if (pageFrameArray.get(current).refbit) {
                        pageFrameArray.get(current).refbit = false;
                    } else {
                        return pageFrameArray.get(current);
                    }
                }
            }
            
            // increment current, mod N
         	current = (current+1) % pageFrameArray.size();
            //current = i % pageFrameArray.size();
        }

        // (-1) if No frame available in the buff. pool
        // TODO: return an error
        return null;
    }
}