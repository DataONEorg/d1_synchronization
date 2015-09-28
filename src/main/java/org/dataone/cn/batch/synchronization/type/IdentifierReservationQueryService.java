package org.dataone.cn.batch.synchronization.type;

import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;

public interface IdentifierReservationQueryService {

    public boolean hasReservation(Session session, Subject subject, Identifier id) 
            throws NotAuthorized, NotFound, IdentifierNotUnique, InvalidRequest;

}
