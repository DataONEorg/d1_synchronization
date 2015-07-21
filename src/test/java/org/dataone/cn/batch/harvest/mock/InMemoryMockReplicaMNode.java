package org.dataone.cn.batch.harvest.mock;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.dataone.client.v2.impl.InMemoryMNode;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.SystemMetadata;

public class InMemoryMockReplicaMNode extends InMemoryMNode {

    public InMemoryMockReplicaMNode(Subject nodeAdmin, Subject cnClientSubject,
            NodeReference coordinatingNode) {
        super(nodeAdmin, cnClientSubject, coordinatingNode);
        // TODO Auto-generated constructor stub
    }
    
    /**
     * A non-API method to simplify getting a replica onto a replica MNode
     * @param object
     * @param sysMeta
     * @return
     * @throws InvalidRequest
     * @throws ServiceFailure
     */
    public Identifier pushReplica(InputStream object, SystemMetadata sysMeta) throws InvalidRequest, ServiceFailure {

            byte[] objectBytes;
            try {
                objectBytes = IOUtils.toByteArray(object);
                this.objectStore.put(sysMeta.getIdentifier(),objectBytes);
                this.metaStore.put(sysMeta.getIdentifier(), sysMeta);
                addToSeries(sysMeta.getSeriesId(), sysMeta.getIdentifier());
                eventLog.add(buildLogEntry(Event.REPLICATE, sysMeta.getIdentifier(), null));
                
            } catch (IOException e) {
                ServiceFailure sf = new ServiceFailure("000",sysMeta.getIdentifier().getValue() +
                        "Problem in create() converting the inputStream to byte[].");
                sf.initCause(e);
                throw sf;
            }

        return sysMeta.getIdentifier();
    }

}
