/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.ldap.impl;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.batch.type.SimpleNode;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.util.DateTimeMarshaller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.ContextMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.DistinguishedName;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.filter.AndFilter;
import org.springframework.ldap.filter.EqualsFilter;
import org.springframework.stereotype.Service;

/**
 * Provide read and write access to the nodelist stored in LDAP
 * @author waltz
 */
@Service
@Qualifier("hazelcastLdapStore")
public class HazelcastLdapStore implements MapLoader, MapStore {

    public static Log log = LogFactory.getLog(HazelcastLdapStore.class);
    @Autowired
    @Qualifier("ldapTemplate")
    private LdapTemplate ldapTemplate;

    @Override
    public Object load(Object key) {
        return getSingleNode((String) key);
    }

    @Override
    public Map loadAll(Collection keyCollection) {
        // Interpret loadAll as a way to get allNode again?
        List<SimpleNode> simpleNodes =  getAllSingleNodes();
        Map<String, SimpleNode> mappedStore = new ConcurrentHashMap<String, SimpleNode>();
        for (SimpleNode simpleNode : simpleNodes) {
            mappedStore.put(simpleNode.getNodeId(), simpleNode);
        }
        return mappedStore;
    }

    @Override
    public Set loadAllKeys() {
        // upon instantiation of this Store, or when the first time
         // a map is called, loadAllKeys will be called.
        List<SimpleNode> simpleNodes =  getAllSingleNodes();
        Set<String> keys =  Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        for (SimpleNode simpleNode : simpleNodes) {
            keys.add(simpleNode.getNodeId());
        }
        return keys;
    }

    @Override
    public void store(Object key, Object value) {
        updateLastHarvested((String) key, (SimpleNode) value);
    }

    @Override
    public void storeAll(Map map) {
        for (Object key : map.keySet()) {
            updateLastHarvested((String) key, (SimpleNode) map.get(key));
        }
    }

    @Override
    public void delete(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteAll(Collection collection) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private void updateLastHarvested(String nodeId, SimpleNode node) {
        DistinguishedName dn = new DistinguishedName();
        dn.add("d1NodeId", nodeId);
        DirContextOperations context = ldapTemplate.lookupContext(dn);
        context.setAttributeValue("d1NodeLastHarvested", DateTimeMarshaller.serializeDateToUTC(node.getLastHarvested()));
        ldapTemplate.modifyAttributes(context);
    }

    /**
     * calls the Spring ldapTemplate search method to map
     * objectClass d1NodeService to dataone Node services
     * and return a list of them in order to compose a Services object
     * to connect to a NodeList object
     *
     * @author rwaltz
     */
    private SimpleNode getSingleNode(String nodeId) {
        // because we use a base DN, only need to supply the RDN
        DistinguishedName dn = new DistinguishedName();
        dn.add("d1NodeId", nodeId);

        return (SimpleNode)ldapTemplate.lookup(dn, new SimpleNodeContextMapper());
    }

    private static class SimpleNodeContextMapper implements ContextMapper {

        public Object mapFromContext(Object ctx) {
            DirContextAdapter context = (DirContextAdapter) ctx;
            SimpleNode node = new SimpleNode();
            node.setNodeId((String) context.getStringAttribute("d1NodeId"));
            node.setBaseUrl((String) context.getStringAttribute("d1NodeBaseURL"));
            node.setLastHarvested(DateTimeMarshaller.deserializeDateToUTC((String) context.getStringAttribute("d1NodeLastHarvested")));
            String seconds = (String) context.getStringAttribute("d1NodeSynSchdSec") ;
            String minutes =   (String) context.getStringAttribute("d1NodeSynSchdMin") ;
            String hours = (String) context.getStringAttribute("d1NodeSynSchdHour")  ;
            String dayOfMonth =  (String) context.getStringAttribute("d1NodeSynSchdMday") ;

            String months =  (String) context.getStringAttribute("d1NodeSynSchdMon") ;
            String dayOfWeek = (String) context.getStringAttribute("d1NodeSynSchdWday") ;
            String years =  (String) context.getStringAttribute("d1NodeSynSchdYear") ;
            node.setCrontab(seconds + " " + minutes + " " + hours + " " + dayOfMonth + " " + months + " " + dayOfWeek + " " + years);
            return node;
        }
    }

    /**
     * calls the Spring ldapTemplate search method to map
     * objectClass d1NodeService to dataone Node services
     * and return a list of them in order to compose a Services object
     * to connect to a NodeList object
     *
     * @author rwaltz
     */
    private List<SimpleNode> getAllSingleNodes() {
        AndFilter filter = new AndFilter();
        filter.and(new EqualsFilter("objectclass", "d1Node"));
        filter.and(new EqualsFilter("d1NodeType", NodeType.MN.xmlValue()));
        filter.and(new EqualsFilter("d1NodeSynchronize", "TRUE"));

        return ldapTemplate.search("", filter.encode(), new SimpleNodeAttributesMapper());
    }

    /**
     * Used in getAllNodes search to map attributes
     * returned from the objectClass of D1Node
     * into to a dataone node class
     *
     * @author rwaltz
     */
    private class SimpleNodeAttributesMapper implements AttributesMapper {

        @Override
        public Object mapFromAttributes(Attributes attrs) throws NamingException {
            SimpleNode node = new SimpleNode();

            node.setNodeId((String) attrs.get("d1NodeId").get());
            node.setBaseUrl((String) attrs.get("d1NodeBaseURL").get());
            node.setLastHarvested(DateTimeMarshaller.deserializeDateToUTC((String) attrs.get("d1NodeLastHarvested").get()));
            String seconds = (String) attrs.get("d1NodeSynSchdSec").get();
            seconds = seconds.replace(" ", "");
            String minutes =   (String) attrs.get("d1NodeSynSchdMin").get();
            minutes = minutes.replace(" ", "");
            String hours = (String) attrs.get("d1NodeSynSchdHour").get();
            hours = hours.replace(" ", "");
            String days =  (String) attrs.get("d1NodeSynSchdMday").get();
            days = days.replace(" ", "");
            String months =  (String) attrs.get("d1NodeSynSchdMon").get();
            months = months.replace(" ", "");
            String weekdays = (String) attrs.get("d1NodeSynSchdWday").get();
            weekdays = weekdays.replace(" ", "");
            String years =  (String) attrs.get("d1NodeSynSchdYear").get();
            years = years.replace(" ", "");
            if (days.equalsIgnoreCase("?") && weekdays.equalsIgnoreCase("?")) {
                // both can not be ?
                days = "*";
            } else if (!(days.equalsIgnoreCase("?")) && !(weekdays.equalsIgnoreCase("?"))) {
             // if both of them are set to something other than ?
             // then one of them needs to be ?
             // if one of them is set as * while the other is not
                if (days.equalsIgnoreCase("*") && weekdays.equalsIgnoreCase("*")) {
                    weekdays = "?";
                } else if (days.equalsIgnoreCase("*")) {
                    days = "?";
                } else {
                    weekdays = "?";
                }
            }
            node.setCrontab(seconds + " " + minutes + " " + hours + " " + days + " " + months + " " + weekdays + " " + years);
            return node;
        }
    }

}
