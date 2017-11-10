package org.dataone.cn.batch.synchronization.tasks;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;


/**
 * This class represents a high-level type for the items retrieved in 
 * a MemberNode harvest.  It controls additions and removals to better
 * manage its memory footprint.
 * 
 * For a harvest, only the identifier and dateSystemMetadataModified
 * are needed from the ObjectInfos (elements of the ObjectList), and these
 * are stored as a map where the keys are the sysmeta modification dates,
 * and the values are the list of identifiers sharing that date.
 * 
 * Setting the lastHarvestedDate for a MemberNode requires that all
 * pids sharing the same dateSystemMetadataModified be part of the 
 * same harvest.  This class is careful to control removals and additions
 * to the Map such that only the latest timepoint can be removed, and
 * once removed, it as well as later timepoints cannot be added at a 
 * later time.
 * 
 * The earliest timepoint in the map cannot be removed, or else the harvest
 * could end up being empty, even though there are objects to harvest.  
 * (For example, if a timepoint is larger than the maximum harvest size,
 * it still needs to be harvested.  The harvest in this case will exceed 
 * its maximum value, because it must complete the timepoint)
 * 
 * 
 * @author rnahf
 *
 */
public class SortedHarvestTimepointMap {
    
    private Date fromDate;
    private Date toDate;
    private Date earliestRemoveDate = null;
    private Integer maxHarvestSize = null;
    private int totalRetainedPids = 0;
    
    /* the managed data structure */
    private TreeMap<Date,List<String>> pidMap = new TreeMap<>();

   
    /**
     * Constructor that accepts optional limits on what gets added
     * @param fromDate - the earliest timepoint accepted
     * @param toDate - the latest timepoint accepted
     */
    public SortedHarvestTimepointMap(Date fromDate, Date toDate, Integer maxHarvestSize) {
        this.fromDate = fromDate;
        this.toDate = toDate;
        this.maxHarvestSize = maxHarvestSize;
    }
    
    
    /**
     * @return the total number of identifiers in the map
     */
    int getHarvestSize() {
        return totalRetainedPids;
    }
    
    /**
     * @return the Date of the latest timepoint in the map, or null if the map is empty
     */
    Date getLatestTimePoint() {
        try {
        return pidMap.lastKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }
    
    /**
     * @return the Date of the earliest timepoint in the map, or null if the map is empty
     */
    Date getEarliestTimePoint() {
        try {
        return pidMap.firstKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    
    /**
     * Adds elements of the ObjectList to the map, if they are in the time window,
     * and are earlier than the removed timepoints
     * 
     * @param ol
     * @return - the the net number of pids added to the Map. DOES NOT include those outside time window,
     * or those subsequently removed due to the maxHarvest size beeing reached!
     */
    void addObjectList(ObjectList ol) {

        for (ObjectInfo oi : ol.getObjectInfoList()) {
            Date smdDate = oi.getDateSysMetadataModified();

            if (fromDate != null && fromDate.after(smdDate))
                continue;

            if (toDate != null && toDate.before(smdDate))
                continue;

            // once a timepoint has been removed, no other items
            // from that timepoint or later can be added, otherwise we create a partial harvest
            if (earliestRemoveDate != null && !smdDate.before(earliestRemoveDate)) 
                continue;

            if (!pidMap.containsKey(smdDate)) {
                pidMap.put(smdDate, new ArrayList<String>());
            }
            pidMap.get(smdDate).add(oi.getIdentifier().getValue());
            totalRetainedPids++;

            if (this.maxHarvestSize != null && totalRetainedPids > this.maxHarvestSize) {
                if (pidMap.size() > 1) {
                    removeLatestTimePoint();       
                } 
                // else 
                //   keep the earliest timepoint, or else an excessively
                //   large timepoints would never get harvested
                //   (and synchronization would stall)
                //   This mechanism will effectively finish the harvest
                //   right before the large timepoint, then harvest the
                //   large timepoint in the next execution.
            }
        }
    }
    
    /**
     * removes the latest timepoint from the data structure, along
     * with associated identifiers.
     * @return
     *    the number of identifiers removed
     */
    private int removeLatestTimePoint() {
       Date latestTime = getLatestTimePoint();
       if (latestTime == null) {
           return 0;
       }
       if (earliestRemoveDate == null) {
           earliestRemoveDate = latestTime;
       } else if (earliestRemoveDate.after(latestTime)) {
           earliestRemoveDate = latestTime;
       }
       
       int removedPidCount = pidMap.remove(latestTime).size();
       totalRetainedPids -= removedPidCount;
       return removedPidCount;
    }
    
    /**
     * The iterator used to start retrieving from the map
     * to feed the synchronization queue
     * 
     * @return
     */
    Iterator<Entry<Date,List<String>>> getAscendingIterator() {
        return pidMap.entrySet().iterator();
    }
    
}
