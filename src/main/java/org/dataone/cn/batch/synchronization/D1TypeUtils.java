package org.dataone.cn.batch.synchronization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.dataone.exceptions.MarshallingException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.util.TypeMarshaller;

/**
 * This class offers direct and efficient equality comparison of DataONE types without having 
 * to do any null-checking prior to the comparison.  The 'equals' methods and 
 * 'serializedFormEquals' method follow the ObjectUtils and StringUtils pattern
 * if returning false if exactly 1 of the objects passed in is null.  
 * 
 * The 'valueEquals' methods will evaluate to true if a null Object is compared 
 * to a null-valued object  (like when identifier.getValue() == null).
 * 
 * TODO: Is there a need for equating empty-string to null and null-valued objects?
 * 
 * @author rnahf
 *
 */
public class D1TypeUtils {

    public D1TypeUtils() {
        // TODO Auto-generated constructor stub
    }
    
    /**
     * A null-safe equality test for two Identifiers
     * 
     * @param id1
     * @param id2
     * @return
     */
    public static boolean equals(Identifier id1, Identifier id2) {
        
        if (id1 == id2)
            return true;
        
        if (id1 == null || id2 == null)
            return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }
    

    /**
     * A null-safe equality test for two NodeReferences
     * 
     * @param id1
     * @param id2
     * @return
     */
    public static boolean equals(NodeReference id1, NodeReference id2) {
        if (id1 == id2)
            return true;
        
        if (id1 == null || id2 == null)
            return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }
    
    /**
     * A null-safe equality test for two ObjectFormatIdentifiers
     * 
     * @param id1
     * @param id2
     * @return
     */
    public static boolean equals(ObjectFormatIdentifier id1, ObjectFormatIdentifier id2) {
        if (id1 == id2)
            return true;
        
        if (id1 == null || id2 == null)
            return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }
    
    /**
     * A null-safe equality test for two Subjects
     * 
     * @param id1
     * @param id2
     * @return
     */
    public static boolean equals(Subject id1, Subject id2) {
        if (id1 == id2)
            return true;
        
        if (id1 == null || id2 == null)
            return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }
    
    /**
     * A null-safe equality test for two Dataone objects that bases the comparison
     * on the equality of their serialized representations.
     * 
     * Note: functionally equivalent objects (those that have unordered lists, for example)
     * may not evaluate as equal.
     *   
     * @param object1
     * @param object2
     * @return
     * @throws MarshallingException
     * @throws IOException
     */
    public static boolean serializedFormEquals(Object object1, Object object2) throws MarshallingException, IOException {
        
        if (object1 == object2)
            return true;
        
        if (object1 == null || object2 == null)
            return false;
        
        ByteArrayOutputStream obj1os = null;
        ByteArrayOutputStream obj2os = null;
        
        try {
            obj1os = new ByteArrayOutputStream();
            obj2os = new ByteArrayOutputStream();

            TypeMarshaller.marshalTypeToOutputStream(object1, obj1os);
            TypeMarshaller.marshalTypeToOutputStream(object2, obj2os);

            return Arrays.equals(obj1os.toByteArray(), obj2os.toByteArray());
        
        } finally {
            IOUtils.closeQuietly(obj1os);
            IOUtils.closeQuietly(obj2os);
        }
    }
 
    /**
     * Similar to equals method, but also returns true if one ID is null, and
     * the other ID's value property is null.
     * @param id1
     * @param id2
     * @return
     */
    public static boolean valueEquals(Identifier id1, Identifier id2) {
        
        if (id1 == id2)
            return true;
        
        if (id1 == null) 
            if (id2.getValue() == null)
                return true;
            else
                return false;
        
        if (id2 == null) 
            if (id1.getValue() == null)
                return true;
            else
                return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }

    /**
     * Similar to equals method, but also returns true if one ID is null, and
     * the other ID's value property is null.
     * @param id1
     * @param id2
     * @return
     */
    public static boolean valueEquals(NodeReference id1, NodeReference id2) {
        
        if (id1 == id2)
            return true;
        
        if (id1 == null) 
            if (id2.getValue() == null)
                return true;
            else
                return false;
        
        if (id2 == null) 
            if (id1.getValue() == null)
                return true;
            else
                return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }

    /**
     * Similar to equals method, but also returns true if one ID is null, and
     * the other ID's value property is null.
     * @param id1
     * @param id2
     * @return
     */
    public static boolean valueEquals(ObjectFormatIdentifier id1, ObjectFormatIdentifier id2) {
        
        if (id1 == id2)
            return true;
        
        if (id1 == null) 
            if (id2.getValue() == null)
                return true;
            else
                return false;
        
        if (id2 == null) 
            if (id1.getValue() == null)
                return true;
            else
                return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }

    /**
     * Similar to equals method, but also returns true if one ID is null, and
     * the other ID's value property is null.
     * @param id1
     * @param id2
     * @return
     */
    public static boolean valueEquals(Subject id1, Subject id2) {
        
        if (id1 == id2)
            return true;
        
        if (id1 == null) 
            if (id2.getValue() == null)
                return true;
            else
                return false;
        
        if (id2 == null) 
            if (id1.getValue() == null)
                return true;
            else
                return false;
        
        if (id1.getValue() == id2.getValue())
            return true;
        
        if (id1.getValue() == null || id2.getValue() == null)
            return false;
        
        return (id1.getValue().equals(id2.getValue()));
    }
 
    /**
     * Similar to equals, but treats a null object, null value-property, and empty string
     * value-property as equal to each other
     * @param id1
     * @param id2
     * @return
     */
    public static boolean emptyEquals(NodeReference id1, NodeReference id2) {

        // deal with all of the null & empty string cases first where:
        // null == null-value == empty
        if (id1 == null || id1.getValue() == null || id1.getValue().equals("")) 
            if (id2 == null || id2.getValue() == null || id2.getValue().equals(""))
                return true;
            else
                return false;

        else if (id2 == null || id2.getValue() == null || id2.getValue().equals(""))
            return false;

        // only non-null and non-empty cases

        if (id1 == id2)
            return true;
        
        return (id1.getValue().equals(id2.getValue()));
    }

    
}

