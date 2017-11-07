package org.dataone.cn.batch.synchronization.tasks;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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
 * once removed, it as well as later timepoints can be added at a 
 * later time.
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
    
    
    private TreeMap<Date,List<String>> pidMap = new TreeMap<>();
        
    int getTotalPids() {
        return totalRetainedPids;
    }
    
    Date getLatestTimePoint() {
        return pidMap.lastKey();
    }
    
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
     * Adds elements of the ObjectList to the map, if they are in the time window,
     * and are earlier than the removed timepoints
     * 
     * @param ol
     * @return - the number of pids added to the Map. DOES NOT include those outside time window!
     */
     int addObjectList(ObjectList ol) {
        
        int added = 0;
        for (ObjectInfo oi : ol.getObjectInfoList()) {
            Date smdDate = oi.getDateSysMetadataModified();
            
            if (fromDate != null && fromDate.after(smdDate))
                continue;
            
            if (toDate != null && toDate.before(smdDate))
                continue;
            
            // once a timepoint has been removed, no other items
            // from that timepoint or later can be added, otherwise we create a partial harvest
            if (!smdDate.before(earliestRemoveDate)) 
                continue;
            
            if (!pidMap.containsKey(smdDate)) {
                pidMap.put(smdDate, new ArrayList<String>());
            }
            pidMap.get(smdDate).add(oi.getIdentifier().getValue());
            added++;
            
            if (totalRetainedPids > this.maxHarvestSize) {
                removeLatestTimePoint();
            }
        }
        totalRetainedPids += added;
        return added;
    }
    
    /**
     * removes the latest timepoint from the data structure, along
     * with associated pids.
     * @return
     */
    int removeLatestTimePoint() {
       if (earliestRemoveDate == null) {
           earliestRemoveDate = getLatestTimePoint();
       } else if (earliestRemoveDate.after(getLatestTimePoint())) {
           earliestRemoveDate = getLatestTimePoint();
       }
       
       int removedPidCount = pidMap.lastEntry().getValue().size();
       pidMap.remove(getLatestTimePoint());
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
