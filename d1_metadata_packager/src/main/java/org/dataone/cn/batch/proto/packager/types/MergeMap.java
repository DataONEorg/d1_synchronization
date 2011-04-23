/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager.types;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This Map contains a state information about processing created or replicated
 * files in metacat.
 *
 * Logging files are parsed for create or replicate actions
 * The actions conform to a specific pattern that matches a Data ONE PID
 * to the identifier of the metacat object.  the PID will crossreferece
 * metacat's systemmetadata id and sciencemetadata id to one D1 PID.
 *
 * The packager needs this crossreference to send a single xml document
 * to the dataone search indexer
 *
 * For each DataONE PID there will be at a minimal two entries:
 *         SCIMETA => metacat internal science metadata id
 *         SYSMETA => metacat internal system metadata id
 *
 * optionally, if the when the writer is called, the entry is not complete,
 * then additional data will be added
 *         EXPIRE_DATE_LONG => string representation of long data date
 *
 * the optional field keeps a timestamp of how long the dateONE PID should be
 * maintained in the persistent cache before expiring.
 *
 * During replication, most DataONE PIDs will not have both SCIMETA and SYSMETA
 * appear at the same time in the log files, therefore, the system will keep attempting
 * to create a complete entry for several hours (currently 12) before it fails
 *
 * Event processing may fail temporarily, and all read data needs to be cached in case the
 * system goes down due to a temporary failure.  The system, then can start up where it
 * left off in processing the event log.
 *
 * 
 *
 * I need this class because of Java erasure of Generics during runtime.
 * Maybe there is a better way, but I need instanceof to ensure that
 * I have retrieved the correct object from the persistent store
 * @author waltz
 */
public class MergeMap extends HashMap implements Map, Serializable {

    private Map<String, Map<String, String>> mergeMap;

    public MergeMap() {
        mergeMap = new HashMap<String, Map<String, String>>();
    }

    @Override
    public int size() {
        return mergeMap.size();
    }

    @Override
    public boolean isEmpty() {
        return mergeMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        if (o instanceof String) {
            return mergeMap.containsKey((String) o);
        } else {
            return false;
        }
    }

    public boolean containsKey(String s) {
        return mergeMap.containsKey((String) s);
    }

    @Override
    public boolean containsValue(Object o) {
        if (o instanceof Map) {
            return mergeMap.containsValue((Map) o);
        } else {
            return false;
        }
    }

    public boolean containsValue(Map<String, String> m) {
        return mergeMap.containsValue(m);
    }

    @Override
    public Object get(Object o) {
        return mergeMap.get((String) o);
    }

    public Map<String, String> get(String s) {
        return mergeMap.get(s);
    }

    @Override
    public Object put(Object k, Object v) {
        return mergeMap.put((String) k, (Map) v);
    }

    public Map<String, String> put(String k, Map<String, String> v) {
        return mergeMap.put(k, v);
    }

    @Override
    public Object remove(Object o) {
        return mergeMap.remove((String) o);
    }

    public Map<String, String> remove(String o) {
        return mergeMap.remove((String) o);
    }

    @Override
    public void putAll(Map map) {
        mergeMap.putAll((Map<String, Map<String, String>>) map);
    }

    @Override
    public void clear() {
        mergeMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return mergeMap.keySet();
    }

    @Override
    public Collection values() {
        return mergeMap.values();
    }

    @Override
    public Set entrySet() {
        return mergeMap.entrySet();
    }

    public Map<String, Map<String, String>> getMap() {
        return mergeMap;
    }

    public void setMap(Map<String, Map<String, String>> mergeMap) {
        this.mergeMap = mergeMap;
    }
}
