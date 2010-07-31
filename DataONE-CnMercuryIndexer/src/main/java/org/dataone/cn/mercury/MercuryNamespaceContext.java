package org.dataone.cn.mercury;


import java.util.LinkedHashMap;
import java.util.Iterator;

import javax.xml.*;
import javax.xml.namespace.NamespaceContext;



public class MercuryNamespaceContext implements NamespaceContext {
	
    private LinkedHashMap<String, String> namespaceHashMap;
	

    public String getNamespaceURI(String prefix) {
     	
        if (prefix == null) throw new NullPointerException("Null prefix");
        else if (namespaceHashMap.containsKey(prefix)) return namespaceHashMap.get(prefix);
        else if ("xml".equals(prefix)) return XMLConstants.XML_NS_URI;
        return XMLConstants.NULL_NS_URI;
    }

    // This method isn't necessary for XPath processing.
    public String getPrefix(String uri) {
        throw new UnsupportedOperationException();
    }

    // This method isn't necessary for XPath processing either.
    public Iterator getPrefixes(String uri) {
        throw new UnsupportedOperationException();
    }

    public LinkedHashMap<String, String> getNamespaceHashMap() {
        return namespaceHashMap;
    }

    public void setNamespaceHashMap(LinkedHashMap<String, String> namespaceHashMap) {
        this.namespaceHashMap = namespaceHashMap;
    }



}
