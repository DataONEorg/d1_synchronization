package org.dataone.cn.batch.synchronization.tasks;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.TypeFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SortedHarvestTimepointMapTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("==========================================");
    }

    @Test
    public void testUnrestrictedMap() {
        long currentTime = System.currentTimeMillis();
        SortedHarvestTimepointMap harvest = new SortedHarvestTimepointMap(null,null,null);
        
        // test the methods prior to adding anything
        assertEquals("Map should be empty before adding anything", 0,harvest.getTotalPids());
        assertNull("latest timepoint should be empty prior to adding anything",harvest.getLatestTimePoint());
        assertNull("earliest timepoint should be empty prior to adding anything",harvest.getEarliestTimePoint());
        
        assertNotNull("the iterator should not be null, even if map is empty", harvest.getAscendingIterator());
        
        
        
        ObjectList ol = new ObjectList();
        for (int i=0; i<10; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*i));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        
        assertEquals("getLatestTimepoint() should return the original currentTime", currentTime, harvest.getLatestTimePoint().getTime());
        assertEquals("getEarliestTimePoint() should return the currentTime - 900", currentTime -900, harvest.getEarliestTimePoint().getTime());
        assertEquals("getTotalPids() should return all 10", 10, harvest.getTotalPids());
        Iterator<Entry<Date,List<String>>> it = harvest.getAscendingIterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            i++;
        }
        assertEquals("getAscendingIterator() should iterate over all 10 timepoints",10,i);
              
        ol = new ObjectList();
        for (int j=0; j<10; j++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("bar."+j));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*j));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        assertEquals("getLatestTimepoint() [2] should now return the original currentTime", currentTime, harvest.getLatestTimePoint().getTime());
        assertEquals("getEarliestTimepoint() [2] should  return the original currentTime - 9 *100", currentTime -900, harvest.getEarliestTimePoint().getTime());

        assertEquals("getTotalPids() should return all 20", 20, harvest.getTotalPids());
        Iterator<Entry<Date,List<String>>> it2 = harvest.getAscendingIterator();
        int j = 0;
        while (it2.hasNext()) {
            Entry<Date,List<String>> en = it2.next();
            System.out.println(en.getKey() + " : " + StringUtils.join(en.getValue(), ','));
            j++;
        }
        assertEquals("getAscendingIterator() [2] should iterate over all 10 timepoints",10,j);       
    }

    @Test
    public void testTimeRestrictedMap() {
        long currentTime = System.currentTimeMillis();
        Date fromDate = new Date(currentTime - 6*100);
        Date toDate = new Date(currentTime);
        SortedHarvestTimepointMap harvest = new SortedHarvestTimepointMap(fromDate,toDate,null);
              
        ObjectList ol = new ObjectList();
        for (int i=0; i<10; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*i));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        
        assertEquals("getLatestTimepoint() should return the original currentTime", currentTime, harvest.getLatestTimePoint().getTime());
        assertEquals("getTotalPids() should return only the 7 of 10 that are in time window", 7, harvest.getTotalPids());
        Iterator<Entry<Date,List<String>>> it = harvest.getAscendingIterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            i++;
        }
        assertEquals("getAscendingIterator() should iterate over all 7 timepoints",7,i);
              
        ol = new ObjectList();
        for (int j=0; j<10; j++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("bar."+j));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*j));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        Iterator<Entry<Date,List<String>>> it2 = harvest.getAscendingIterator();
        int j = 0;
        while (it2.hasNext()) {
            Entry<Date,List<String>> en = it2.next();
            System.out.println(en.getKey() + " : " + StringUtils.join(en.getValue(), ','));
            j++;
        }
        assertEquals("getAscendingIterator() [2] should iterate over all 7 timepoints",7,j); 
        assertEquals("getLatestTimepoint() [2] should still return the original currentTime", currentTime, harvest.getLatestTimePoint().getTime());
        assertEquals("getTotalPids() should return 7 * 2 = 14", 14, harvest.getTotalPids());
    }
    
    
    @Test
    public void testHarvestSizeRestrictedMap() {
        long currentTime = System.currentTimeMillis();
        
        SortedHarvestTimepointMap harvest = new SortedHarvestTimepointMap(null,null,7);
              
        ObjectList ol = new ObjectList();
        for (int i=0; i<10; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*i));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        
        assertEquals("getLatestTimepoint() should return the original currentTime minus 3 * 100", currentTime-300, harvest.getLatestTimePoint().getTime());
        assertEquals("getTotalPids() should return only the 7 of 10 that are in harvest size", 7, harvest.getTotalPids());
        Iterator<Entry<Date,List<String>>> it = harvest.getAscendingIterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            i++;
        }
        assertEquals("getAscendingIterator() should iterate over all 7 timepoints",7,i);
              
        ol = new ObjectList();
        for (int j=0; j<10; j++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("bar."+j));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*j));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        
        Iterator<Entry<Date,List<String>>> it2 = harvest.getAscendingIterator();
        int j = 0;
        while (it2.hasNext()) {
            Entry<Date,List<String>> en = it2.next();
            System.out.println(en.getKey().getTime() + " : " + StringUtils.join(en.getValue(), ','));
            j++;
        }
        assertEquals("getAscendingIterator() [2] should iterate over 3 timepoints (with 2 identifiers each)",3,j);
        assertEquals("getLatestTimepoint() [2] should still return the original currentTime - 7 *100", currentTime -700, harvest.getLatestTimePoint().getTime());
        assertEquals("getTotalPids() should return 3*2", 6, harvest.getTotalPids());
    }
    
    @Test
    public void testFullyRestrictedMap() {
        long currentTime = System.currentTimeMillis();
        Date fromDate = new Date(currentTime - 8*100);
        Date toDate = new Date(currentTime - 1*100);
        SortedHarvestTimepointMap harvest = new SortedHarvestTimepointMap(fromDate,toDate,17);
              
        ObjectList ol = new ObjectList();
        for (int i=0; i<10; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*i));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        
        assertEquals("getLatestTimepoint() should return the original currentTime minus 1 * 100", currentTime-100, harvest.getLatestTimePoint().getTime());
        assertEquals("getTotalPids() should return only the 8 of 10 that are in time window", 8, harvest.getTotalPids());
        Iterator<Entry<Date,List<String>>> it = harvest.getAscendingIterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            i++;
        }
        assertEquals("getAscendingIterator() should iterate over all 8 timepoints",8,i);
              
        ol = new ObjectList();
        for (int j=0; j<10; j++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("bar."+j));
            oi.setDateSysMetadataModified(new Date(currentTime - 100*j));
            ol.addObjectInfo(oi);
        }
        harvest.addObjectList(ol);
        
        
        Iterator<Entry<Date,List<String>>> it2 = harvest.getAscendingIterator();
        int j = 0;
        while (it2.hasNext()) {
            Entry<Date,List<String>> en = it2.next();
            System.out.println(en.getKey().getTime() + " : " + StringUtils.join(en.getValue(), ','));
            j++;
        }
        assertEquals("getAscendingIterator() [2] should iterate over 8 timepoints (with 2 identifiers each)",8,j);
        assertEquals("getLatestTimepoint() [2] should still return the original currentTime - 1 *100", currentTime -100, harvest.getLatestTimePoint().getTime());
        assertEquals("getEarliestTimepoint() [2] should still return the original currentTime - 8 *100", currentTime -800, harvest.getEarliestTimePoint().getTime());
        assertEquals("getTotalPids() should return 8*2", 16, harvest.getTotalPids());
    }

}
