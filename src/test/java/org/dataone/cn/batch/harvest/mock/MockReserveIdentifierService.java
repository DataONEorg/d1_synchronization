package org.dataone.cn.batch.harvest.mock;

import javax.naming.NamingException;

import org.dataone.cn.batch.synchronization.type.IdentifierReservationQueryService;
import org.dataone.service.cn.impl.v2.ReserveIdentifierService;
import org.dataone.service.exceptions.*;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;

public class MockReserveIdentifierService implements IdentifierReservationQueryService {

    private Identifier knownIdentifier;
    private boolean alreadyCreated;
    private Subject reservationHolder;
    private boolean acceptSession;

    public MockReserveIdentifierService(Identifier knownIdentifier, Subject reservationHolder, boolean alreadyCreated, boolean acceptSession) {
        this.knownIdentifier = knownIdentifier;
        this.alreadyCreated = alreadyCreated;
        this.reservationHolder = reservationHolder;
        this.acceptSession = acceptSession;
    }

    public Identifier generateIdentifier(Session session, String scheme, String fragment)
            throws InvalidRequest, ServiceFailure, NotAuthorized {
        throw new ServiceFailure("zzzz","method no implemented in mock ReserveIDentifierClass");
    }

    public boolean removeReservation(Session session, Identifier pid)
            throws NotAuthorized, NotFound, IdentifierNotUnique {

        authorize();
        validateIdentifier(pid);

        this.knownIdentifier = null;
        this.reservationHolder = null;
        return true;
    }



    public boolean hasReservation(Session session, Subject subject, Identifier pid)
            throws NotAuthorized, NotFound, IdentifierNotUnique {

        authorize();
        validateIdentifier(pid);

        if (alreadyCreated)
            throw new IdentifierNotUnique("zzz","Identifier is already in use (already associated with registered object)");

        return subject.equals(reservationHolder);
    }



    private void authorize() throws NotAuthorized {
        if (!acceptSession)
            throw new NotAuthorized("zzz", "session not authorized");
    }


    private void validateIdentifier(Identifier id) throws NotFound {
        if (!id.getValue().equals(this.knownIdentifier.getValue()))
            throw new NotFound("zzz","Identifier not found!!");
    }




    /** do not need to implement for sync testing, is a no-op */
    public void expireEntries(int numberOfDays) throws NamingException {}

    /** do not need to implement for sync testing, is a no=op */
    public static void schedule(final ReserveIdentifierService service) {}

}
