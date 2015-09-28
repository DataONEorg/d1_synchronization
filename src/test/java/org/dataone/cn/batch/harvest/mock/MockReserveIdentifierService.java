package org.dataone.cn.batch.harvest.mock;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.NamingException;

import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.tasks.V2TransferObjectTask;
import org.dataone.cn.batch.synchronization.type.IdentifierReservationQueryService;
import org.dataone.service.cn.impl.v2.ReserveIdentifierService;
import org.dataone.service.cn.v2.CNRead;
import org.dataone.service.exceptions.*;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;

public class MockReserveIdentifierService implements IdentifierReservationQueryService {

    Logger logger = Logger.getLogger(MockReserveIdentifierService.class.getName());
    
    private boolean acceptSession;
    private Map<Identifier, Subject> reservationMap;
    private CNRead cnRead;

    public MockReserveIdentifierService(boolean acceptSession, CNRead cnRead) {
        
        reservationMap = new HashMap<>();
        this.acceptSession = acceptSession;
        this.cnRead = cnRead;
    }
    
    public Identifier reserveIdentifier(Subject subject, Identifier id) throws NotAuthorized, InvalidRequest, IdentifierNotUnique {
        
        Session s = new Session();
        s.setSubject(subject);
        return reserveIdentifier(s, id);
    }
    
    public Identifier reserveIdentifier(Session session, Identifier id) throws NotAuthorized, InvalidRequest, IdentifierNotUnique {

        logger.info("entering reserveIdentifier...");
        try {
            if (hasReservation(session, session.getSubject(), id)) {
                logger.info("already reserved...");
            } else {
                logger.info("no reservation, so adding one...");
                reservationMap.put(id, session.getSubject());
            }
        } catch (NotFound e) {
            logger.info("adding reservation...");
            reservationMap.put(id, session.getSubject());
            
        }
        return id;
    }
    
    public void objectCreated(Identifier pid) {
        
    }

    public Identifier generateIdentifier(Session session, String scheme, String fragment)
            throws InvalidRequest, ServiceFailure, NotAuthorized {
        throw new ServiceFailure("zzzz","method no implemented in mock ReserveIDentifierClass");
    }

    public boolean removeReservation(Session session, Identifier id)
            throws NotAuthorized, NotFound, IdentifierNotUnique, InvalidRequest {
        
        logger.info("entering removeReservation...");
        authorize();
//        validateIdentifier(pid);
        if (hasReservation(session, session.getSubject(), id)) 
            reservationMap.remove(id);

        return true;
    }


    /**
     * @param session - the requester of the hasReservation service, ignored for this implementation.
     * @param subject - the subject used for evaluating the reservation
     * @param id - the pid or sid being checked
     * @throws NotAuthorized - if the existing reservation is not held by the subject parameter
     * @throws NotFound - the identifier doesn't exist as a reservation or object
     * @throws IdentifierNotUnique - the id belongs to an existing object
     */
    public boolean hasReservation(Session session, Subject subject, Identifier id)
            throws NotAuthorized, NotFound, IdentifierNotUnique, InvalidRequest {

        logger.info("entering hasReservation...");
        authorize();
        
        if (subject == null) {
            throw new InvalidRequest("4926", "subject parameter cannot be null");
        }
        if (id == null) {
            throw new InvalidRequest("4926", "pid parameter cannot be null");
        }
        
        try {
            ObjectLocationList oll = this.cnRead.resolve(session, id);
            throw new IdentifierNotUnique("zzz","The identifier is in use in an existing object");
        } catch (InvalidToken | ServiceFailure | NotImplemented e) {
            logger.error("could not properly check object list in hasReservation", e);
        } catch (NotFound e) {
            ; // this is good
        }
        
        if (reservationMap.containsKey(id)) {
            Subject resHolder = reservationMap.get(id);
            if (resHolder.equals(session.getSubject())) {
                return true;
            } else {
                throw new NotAuthorized("4180", "Identifier (" + id.getValue() + 
                    ") is reserved and not owned by subject, " + session.getSubject().getValue());
            }
        } else {
            throw new NotFound("zzz","No reservation on the id");
        }
    }



    private void authorize() throws NotAuthorized {
        if (!acceptSession)
            throw new NotAuthorized("zzz", "session not authorized");
    }


//    private void validateIdentifier(Identifier id) throws NotFound {
//       
//        if (id.getValue().equals(this.knownIdentifier.getValue()))
//            return;
//        if (id.getValue().equals(this.knownSid.getValue()))
//            return;
//        
//        throw new NotFound("zzz","Identifier not found!!");
//    }




    /** do not need to implement for sync testing, is a no-op */
    public void expireEntries(int numberOfDays) throws NamingException {}

    /** do not need to implement for sync testing, is a no=op */
    public static void schedule(final ReserveIdentifierService service) {}

}
