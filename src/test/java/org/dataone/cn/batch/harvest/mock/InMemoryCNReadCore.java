package org.dataone.cn.batch.harvest.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.client.D1Node;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.formats.ObjectFormatCache;
import org.dataone.service.cn.v2.CNCore;
import org.dataone.service.cn.v2.CNRead;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidCredentials;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedMetadataType;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.ChecksumAlgorithmList;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Group;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Person;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.dataone.service.types.v1.util.AuthUtils;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;
import org.dataone.service.types.v2.Log;
import org.dataone.service.types.v2.LogEntry;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.ObjectFormatList;
import org.dataone.service.types.v2.OptionList;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Built primarily for testing, this class is an MNode implementation that
 * holds all of its content as SystemMetadata, LogEntries, and byte arrays (for
 * data objects)
 *
 * All methods return either the expected type or throw a NotImplemented exception.
 * Users should be cautious about object size.
 *
 * @author rnahf
 *
 */
public class InMemoryCNReadCore implements CNode {
    private NodeReference nodeId;

    protected Map<Identifier, byte[]> objectStore;
    protected Map<Identifier, SystemMetadata> metaStore;
    protected IMap<Identifier,SystemMetadata> hzSysMetaMap;
    protected Map<Identifier, Set<Identifier>> seriesMap;
    protected List<LogEntry> eventLog;

    protected Subject nodeAdministrator;
    protected Subject cnClientUser;

    /**
     * Instantiate a new InMemoryCNReadCore.  If the cnClientSubject is null, then
     * getLogRecords will authorize anyone.
     *
     * @param nodeAdmin - the Subject of this Node's administrator
     * @param cnClientSubject - the accepted Subject of the CN.
     */
    public InMemoryCNReadCore(Subject nodeAdmin, Subject cnClientSubject) {
        /* the subjects */
        this.nodeAdministrator = nodeAdmin;
        this.cnClientUser = cnClientSubject;


        /* the collections */
        this.objectStore = new HashMap<Identifier, byte[]>();
        this.metaStore = new HashMap<Identifier,SystemMetadata>();
        this.seriesMap = new HashMap<Identifier, Set<Identifier>>();
        this.eventLog = new ArrayList<LogEntry>();
        
    }
    
    public void setHzSysMetaMap(IMap<Identifier,SystemMetadata> hzSysMetaMap) {
        this.hzSysMetaMap = hzSysMetaMap;
    }

    protected synchronized LogEntry buildLogEntry(String eventString, Identifier pid, Session session) {
        LogEntry le = new LogEntry();
        le.setDateLogged(new Date());
        le.setEvent(eventString);
        le.setIdentifier(pid);
        le.setNodeIdentifier(getNodeId());
        le.setSubject(session.getSubject());
        le.setEntryId(String.format("%ddd", eventLog.size()+1));
        return le;
    }

    protected synchronized LogEntry buildLogEntry(Event event, Identifier pid, Session session) {
        return buildLogEntry(event.toString(), pid, session);
    }


    private SystemMetadata checkAvailableAndAuthorized(Session session, Identifier id, Permission perm)
    throws NotAuthorized, NotFound
    {
        SystemMetadata sysmeta = metaStore.get(id);
        if (sysmeta == null) {
            sysmeta = getSeriesHead(id);
        }
        if (sysmeta == null) {
            throw new NotFound("000",
                    String.format("Object with id %s could not be found",
                            id.getValue())
                    );
        }
        Set<Subject> sessionSubjects = AuthUtils.authorizedClientSubjects(session);
        if (sessionSubjects.contains(this.nodeAdministrator) ||
                sessionSubjects.contains(this.cnClientUser)) {
            return sysmeta;
        }
        if (!AuthUtils.isAuthorized(sessionSubjects, perm, sysmeta)) {
            throw new NotAuthorized("000",String.format("Caller does not have %s" +
                    " permission on %s",
                    perm.xmlValue(),
                    sysmeta.getIdentifier().getValue()));
        }
        return sysmeta;
    }

    private SystemMetadata getSeriesHead(Identifier id) throws NotFound
    {
        Set<Identifier> pidSet = seriesMap.get(id);
        if (pidSet == null) {
            throw new NotFound("000","Identifer not found (" + id.getValue() + ")");
        }
        Iterator<Identifier> it = pidSet.iterator();
        SystemMetadata latest = null;
        Date date = null;
        while (it.hasNext()) {
            SystemMetadata smd = metaStore.get(it.next());
            if (smd != null && smd.getDateUploaded().after(date)) {
                latest = smd;
                date = smd.getDateUploaded();
            }
        }
        return latest;
    }


    private void addToSeries(Identifier series, Identifier pid)
    throws InvalidRequest
    {
        if (pid == null) {
            throw new InvalidRequest("000","Cannot map a null pid to a series!!");
        }
        // if series is null, there is nothing to add
        if (series != null) {
            if (!this.seriesMap.containsKey(series)) {
                HashSet<Identifier> set = new HashSet<Identifier>();
                this.seriesMap.put(series, set);
            }
            this.seriesMap.get(series).add(pid);
        }
    }

    /**
     * Validate that the systemMetadata follows the D1_Schema definitions
     * Doing it through serialization and deserialization (probably a bit overkill)
     *
     * @param sysmeta
     * @throws InvalidSystemMetadata
     */
    private void validateSystemMetadata(SystemMetadata sysmeta)
    throws InvalidSystemMetadata
    {
        Exception caught = null;
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream(512);
            TypeMarshaller.marshalTypeToOutputStream(sysmeta, os);
            os.close();
//            // maybe we don't need to reconstitute to validate...
            TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    new ByteArrayInputStream(os.toByteArray()) );
        } catch (JiBXException e) {
            caught = e;
        } catch (IOException e) {
            caught = e;
        } catch (InstantiationException e) {
            caught = e;
        } catch (IllegalAccessException e) {
            caught = e;
        }
        if (caught != null) {
            InvalidSystemMetadata be = new InvalidSystemMetadata("000","The SystemMetadata is invalid");
            be.initCause(caught);
            throw be;
        }
    }

    /**
     * We will use the NodeId value for the base service URL, too.
     */
    @Override
    public String getNodeBaseServiceUrl() {

        return this.nodeId.getValue();
    }

    @Override
    public NodeReference getNodeId() {
        return this.nodeId;
    }

    @Override
    public void setNodeId(NodeReference nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void setNodeType(NodeType nodeType) {
        // don't do anything
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.MN;
    }

    @Override
    public String getLatestRequestUrl() {
        return "No request info available";
    }




    @Override
    public SystemMetadata getSystemMetadata(Session session, Identifier id)
            throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
            NotFound
    {
        return checkAvailableAndAuthorized(session, id, Permission.READ);
    }


    @Override
    public DescribeResponse describe(Session session, Identifier id)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        SystemMetadata sysmeta =  checkAvailableAndAuthorized(session, id, Permission.READ);
        return new DescribeResponse(
                    sysmeta.getFormatId(),
                    sysmeta.getSize(),
                    sysmeta.getDateSysMetadataModified(),
                    sysmeta.getChecksum(),
                    sysmeta.getSerialVersion());

    }

    /**
     * This method calculates the checksum afresh every call;
     */
    @Override
    public Checksum getChecksum(Session session, Identifier id) throws InvalidToken,
            NotAuthorized, NotImplemented, ServiceFailure, NotFound {

        SystemMetadata smd = checkAvailableAndAuthorized(session, id, Permission.READ);
        return smd.getChecksum();
    }


    @Override
    public ObjectList listObjects(Session session, Date fromDate, Date toDate,
            ObjectFormatIdentifier formatid, NodeReference nodeId, Identifier id,
            Integer start, Integer count)
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {
        ObjectList ol = new ObjectList();
        TreeMap<String, ObjectInfo> oiMap = new TreeMap<String,ObjectInfo>();
        for(Entry<Identifier, SystemMetadata> en : metaStore.entrySet()) {
            SystemMetadata s = en.getValue();
            if (fromDate != null && s.getDateSysMetadataModified().before(fromDate))
                continue;
            if (toDate != null && s.getDateSysMetadataModified().after(toDate))
                continue;
            if (toDate != null && s.getDateSysMetadataModified().equals(toDate))
                continue;
            if (formatid != null && !s.getFormatId().equals(formatid))
                continue;
            if (nodeId != null && !s.getAuthoritativeMemberNode().equals(nodeId))
                continue;
            if (id != null && !(s.getIdentifier().equals(id) || s.getSeriesId().equals(id)) )
                continue;

            // survived the filters
            ObjectInfo oi = new ObjectInfo();
            oi.setChecksum(s.getChecksum());
            oi.setDateSysMetadataModified(s.getDateSysMetadataModified());
            oi.setFormatId(s.getFormatId());
            oi.setIdentifier(s.getIdentifier());
            oi.setSize(s.getSize());
            oiMap.put(en.getKey().getValue(), oi);
        }

        if (!oiMap.isEmpty()) {
            if (count == null || count != 0) {
                Iterator<String> it = oiMap.keySet().iterator();
                int first = start == null ? 0 : start;
                int i = -1;
                while (i + 1 < first) {
                    it.next();
                    i++;
                }
                int c = 0;
                while (it.hasNext() && count > c) {
                    String s = it.next();
                    ol.addObjectInfo(oiMap.get(s));
                    c++;
                }
            }
        }
        return ol;
    }



    @Override
    public InputStream get(Session session, Identifier id)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
            NotFound
    {
        // TODO handle SIDs??
        SystemMetadata smd = checkAvailableAndAuthorized(session, id, Permission.READ);
        Identifier pid = smd.getIdentifier();
        if (objectStore.containsKey(pid)) {
              eventLog.add(buildLogEntry(Event.READ, id, session));
              return new ByteArrayInputStream(objectStore.get(pid));
        }
        throw new NotFound("000",
                String.format("Object with id '%s' could not be found",
                        id.getValue())
                );
    }



    public boolean isAuthorized(Session session, Identifier pid,
            Permission action) throws ServiceFailure, InvalidRequest,
            InvalidToken, NotFound, NotAuthorized, NotImplemented {

        checkAvailableAndAuthorized(session, pid, action);
        return true;
    }


    public Identifier create(Session session, Identifier pid, InputStream object, SystemMetadata sysmeta)
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
            InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
            ServiceFailure, UnsupportedType
    {
        try {
            checkAvailableAndAuthorized(session, pid, Permission.READ);
            // not good
            throw new IdentifierNotUnique("000", pid.getValue() +
                    ": An object with this identifier already exists.");
        } catch (NotFound nf) {
            // good
            byte[] objectBytes;
            try {
                objectBytes = IOUtils.toByteArray(object);
                this.objectStore.put(pid,objectBytes);
                sysmeta.setDateUploaded(new Date());
                sysmeta.setOriginMemberNode(getNodeId());
                validateSystemMetadata(sysmeta);
                putSystemMetadata(pid, sysmeta);
                addToSeries(sysmeta.getSeriesId(), pid);
                eventLog.add(buildLogEntry(Event.CREATE, pid, session));

            } catch (IOException e) {
                ServiceFailure sf = new ServiceFailure("000",pid.getValue() +
                        "Problem in create() converting the inputStream to byte[].");
                sf.initCause(e);
                throw sf;
            }

        } catch (NotAuthorized na) {
            // not good
            throw new IdentifierNotUnique("000", pid.getValue() +
                    ": An object with this identifier already exists.");
        }

        return pid;
    }


    public Identifier update(Session session, Identifier pid,
            InputStream object, Identifier newPid, SystemMetadata sysmeta)
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
            InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
            ServiceFailure, UnsupportedType, NotFound
            {

        // throws exception if the pid being update (obsoleted) can't be found
        // or the requester doesn't have the permissions to do the update
        checkAvailableAndAuthorized(session, pid, Permission.CHANGE_PERMISSION);


        // rule out existence of newPid
        try {
            checkAvailableAndAuthorized(session, newPid, Permission.READ);

            // newPid was found...
            throw new IdentifierNotUnique("000", pid.getValue() +
                    ": An object with this identifier already exists.");
        }
        catch (NotAuthorized na) {
            throw new IdentifierNotUnique("000", pid.getValue() +
                ": An object with this identifier already exists.");
        }
        catch (NotFound nf) {

            byte[] objectBytes;
            try {
                objectBytes = IOUtils.toByteArray(object);
                this.objectStore.put(newPid,objectBytes);
                sysmeta.setDateUploaded(new Date());
                sysmeta.setOriginMemberNode(getNodeId());
                sysmeta.setAuthoritativeMemberNode(getNodeId());
                sysmeta.setDateSysMetadataModified(new Date());
                sysmeta.setObsoletes(pid);
                ObjectFormat of = ObjectFormatCache.getInstance().getFormat(sysmeta.getFormatId());
                if (of == null) {
                    throw new UnsupportedType("000","Cannot store data of the format " +
                            sysmeta.getFormatId().getValue());
                }
                validateSystemMetadata(sysmeta);

                SystemMetadata oldSysMeta = metaStore.get(pid);
                if (oldSysMeta.getArchived()) {
                    throw new InvalidRequest("000","Cannot update an archived object. pid = "
                            + pid.getValue());
                }
                oldSysMeta.setObsoletedBy(newPid);
                oldSysMeta.setDateSysMetadataModified(new Date());

                putSystemMetadata(newPid, sysmeta);
                addToSeries(sysmeta.getSeriesId(), newPid);
                eventLog.add(buildLogEntry(Event.UPDATE, pid, session));

            } catch (IOException e) {
                ServiceFailure sf = new ServiceFailure("000",pid.getValue() +
                        "Problem in update() converting the inputStream to byte[].");
                sf.initCause(e);
                throw sf;
            }
        }
        return pid;
    }


    public Identifier delete(Session session, Identifier id)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        SystemMetadata smd = checkAvailableAndAuthorized(session, id, Permission.CHANGE_PERMISSION);
        Identifier pid = smd.getIdentifier();
        // keep the system metadata
        // TODO: what is the semantics of archived, here?
        archive(session, pid);
        // remove the object
        objectStore.remove(pid);
        // remove the pid from the sid map
        if (!id.equals(pid)) {
            this.seriesMap.get(id).remove(smd);
        }

        eventLog.add(buildLogEntry(Event.DELETE, pid, session));

        return pid;
    }


    @Override
    public InputStream query(Session session, String queryEngine, String query)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented, NotFound {
        //TODO implement
        throw new NotImplemented("000","query is not implemented.");
    }

    @Override
    public QueryEngineDescription getQueryEngineDescription(Session session, String queryEngine)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
            NotFound {
        //TODO implement
        throw new NotImplemented("000","getQueryEngineDescription is not implemented.");
    }

    @Override
    public QueryEngineList listQueryEngines(Session session) throws InvalidToken,
            ServiceFailure, NotAuthorized, NotImplemented {
        //TODO implement
        throw new NotImplemented("000","listQueryEngines is not implemented.");
    }

    @Override
    public boolean updateSystemMetadata(Session session, Identifier pid,
            SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
            ServiceFailure, InvalidRequest, InvalidSystemMetadata,
            InvalidToken {
        // TODO implement
        throw new NotImplemented("000","updateSystemMetadata is not implemented.");
    }


    @Override
    public ObjectLocationList resolve(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement resolve");
    }


    @Override
    public ObjectList search(Session session, String queryType, String query)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement search method");
    }

    @Override
    public Date ping() throws NotImplemented, ServiceFailure,
            InsufficientResources
    {
        return new Date();
    }

    @Override
    public ObjectFormatList listFormats() throws ServiceFailure, NotImplemented
    {
        return ObjectFormatCache.getInstance().listFormats();
    }

    @Override
    public ObjectFormat getFormat(ObjectFormatIdentifier formatid)
            throws ServiceFailure, NotFound, NotImplemented, InvalidRequest
    {
        return ObjectFormatCache.getInstance().getFormat(formatid);
    }

    @Override
    public ObjectFormatIdentifier addFormat(Session session,
            ObjectFormatIdentifier formatid, ObjectFormat format)
            throws ServiceFailure, NotFound, NotImplemented, InvalidRequest,
            NotAuthorized, InvalidToken
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement addFormat method");
    }

    @Override
    public ChecksumAlgorithmList listChecksumAlgorithms()
            throws ServiceFailure, NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement listChecksumAlg method");
    }

    @Override
    public Log getLogRecords(Session session, Date fromDate, Date toDate,
            String event, String pidFilter, Integer start, Integer count)
            throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized,
            NotImplemented, InsufficientResources
    {
     // restrict access to the CN client user subject
        if (this.cnClientUser != null) {
            Set<Subject> requestSubjects = AuthUtils.authorizedClientSubjects(session);

            if (!requestSubjects.contains(this.cnClientUser)) {
                throw new NotAuthorized("000", "The requestor's session does not contain" +
                        "the known CN Client Subject: " + this.cnClientUser.getValue());
            }
        }

        Log result = new Log();
        List<LogEntry> filteredLogs = new ArrayList<LogEntry>();

        for(LogEntry en : eventLog) {
            if (event != null && !en.getEvent().equals(event))
                continue;
            if (fromDate != null && en.getDateLogged().before(fromDate))
                continue;
            if (toDate != null && en.getDateLogged().after(toDate))
                continue;
            if (toDate != null && en.getDateLogged().equals(toDate))
                continue;
            if (pidFilter != null && !en.getIdentifier().getValue().startsWith(pidFilter))
                continue;

            // survived the filters
            filteredLogs.add(en);
        }

        int fromIndex = start == null ? 0 : start;

        int toIndex = filteredLogs.size();
        if (count != null && (start + count < filteredLogs.size())) {
            toIndex = start + count;
        }
        // note: to subList the entire list, use l.subList(0,l.size())
        for (LogEntry le : filteredLogs.subList(fromIndex, toIndex)) {
            result.addLogEntry(le);
        }
        return result;
    }

    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement listNodes method");
    }

    @Override
    public Identifier reserveIdentifier(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized,
            IdentifierNotUnique, NotImplemented, InvalidRequest
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement reserveIdentifier method");
    }

    @Override
    public Node getCapabilities() throws NotImplemented, ServiceFailure
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement getCapabilities method");
    }

    @Override
    public Identifier generateIdentifier(Session session, String scheme,
            String fragment) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotImplemented, InvalidRequest
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean hasReservation(Session session, Subject subject,
            Identifier id) throws InvalidToken, ServiceFailure, NotFound,
            NotAuthorized, NotImplemented, InvalidRequest, IdentifierNotUnique
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method, see MockReserverIdService");
    }

    @Override
    public Identifier registerSystemMetadata(Session session, Identifier pid,
            SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
            ServiceFailure, InvalidRequest, InvalidSystemMetadata, InvalidToken
    {
        try {
            checkAvailableAndAuthorized(session, pid, Permission.READ);
        } catch (NotFound nf) {
            // technically not good, but maybe it's ok 
//          throw new IdentifierNotUnique("000", pid.getValue() +
//          ": An object with this identifier already exists.");
        }
        validateSystemMetadata(sysmeta);
        putSystemMetadata(pid, sysmeta);
        addToSeries(sysmeta.getSeriesId(), pid);
        eventLog.add(buildLogEntry("RegisterSysMeta", pid, session));

        return pid;
    }

    @Override
    public boolean synchronize(Session session, Identifier pid)
            throws NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidSystemMetadata, InvalidToken
    {
        throw new NotImplemented("zzz", "InMemoryCNReadCore hasn't implement method synchronize");
    }

    @Override
    public boolean setObsoletedBy(Session session, Identifier pid,
            Identifier obsoletedByPid, long serialVersion)
            throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidToken, VersionMismatch
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public Identifier archive(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }


    // Non CNCore or CNRead methods are below, none to be implemented



    @Override
    public Identifier setRightsHolder(Session session, Identifier id,
            Subject userId, long serialVersion) throws InvalidToken,
            ServiceFailure, NotFound, NotAuthorized, NotImplemented,
            InvalidRequest, VersionMismatch
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean setAccessPolicy(Session session, Identifier id,
            AccessPolicy policy, long serialVersion) throws InvalidToken,
            NotFound, NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, VersionMismatch
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public Subject registerAccount(Session session, Person person)
            throws ServiceFailure, NotAuthorized, IdentifierNotUnique,
            InvalidCredentials, NotImplemented, InvalidRequest, InvalidToken
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public Subject updateAccount(Session session, Person person)
            throws ServiceFailure, NotAuthorized, InvalidCredentials,
            NotImplemented, InvalidRequest, InvalidToken, NotFound
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean verifyAccount(Session session, Subject subject)
            throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken,
            InvalidRequest, NotFound
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public SubjectInfo getSubjectInfo(Session session, Subject subject)
            throws ServiceFailure, NotAuthorized, NotImplemented, NotFound,
            InvalidToken
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public SubjectInfo listSubjects(Session session, String query,
            String status, Integer start, Integer count) throws InvalidRequest,
            ServiceFailure, InvalidToken, NotAuthorized, NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean mapIdentity(Session session, Subject primarySubject,
            Subject secondarySubject) throws ServiceFailure, InvalidToken,
            NotAuthorized, NotFound, NotImplemented, InvalidRequest,
            IdentifierNotUnique
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean requestMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest, IdentifierNotUnique
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean confirmMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public SubjectInfo getPendingMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean denyMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean removeMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public Subject createGroup(Session session, Group group)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented,
            IdentifierNotUnique, InvalidRequest
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean updateGroup(Session session, Group group)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean updateNodeCapabilities(Session session,
            NodeReference nodeid, Node node) throws NotImplemented,
            NotAuthorized, ServiceFailure, InvalidRequest, NotFound,
            InvalidToken
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public NodeReference register(Session session, Node node)
            throws NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidToken, IdentifierNotUnique
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public Node getNodeCapabilities(NodeReference nodeid)
            throws NotImplemented, ServiceFailure, InvalidRequest, NotFound
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean setReplicationStatus(Session session, Identifier pid,
            NodeReference nodeRef, ReplicationStatus status,
            BaseException failure) throws ServiceFailure, NotImplemented,
            InvalidToken, NotAuthorized, InvalidRequest, NotFound
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean setReplicationPolicy(Session session, Identifier pid,
            ReplicationPolicy policy, long serialVersion)
            throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidToken, VersionMismatch
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean isNodeAuthorized(Session session, Subject targetNodeSubject,
            Identifier pid) throws NotImplemented, NotAuthorized, InvalidToken,
            ServiceFailure, NotFound, InvalidRequest
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean updateReplicationMetadata(Session session, Identifier pid,
            Replica replicaMetadata, long serialVersion) throws NotImplemented,
            NotAuthorized, ServiceFailure, NotFound, InvalidRequest,
            InvalidToken, VersionMismatch
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public boolean deleteReplicationMetadata(Session session, Identifier pid,
            NodeReference nodeId, long serialVersion) throws InvalidToken,
            InvalidRequest, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented, VersionMismatch
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public InputStream view(Session session, String theme, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented, NotFound
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public OptionList listViews() throws InvalidToken, ServiceFailure,
            NotAuthorized, InvalidRequest, NotImplemented
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public SubjectInfo echoCredentials(Session session) throws NotImplemented,
            ServiceFailure, InvalidToken
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public SystemMetadata echoSystemMetadata(Session session,
            SystemMetadata sysmeta) throws NotImplemented, ServiceFailure,
            NotAuthorized, InvalidToken, InvalidRequest, IdentifierNotUnique,
            InvalidSystemMetadata
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }

    @Override
    public InputStream echoIndexedObject(Session session, String queryEngine,
            SystemMetadata sysmeta, InputStream object) throws NotImplemented,
            ServiceFailure, NotAuthorized, InvalidToken, InvalidRequest,
            InvalidSystemMetadata, UnsupportedType, UnsupportedMetadataType,
            InsufficientResources
    {
        throw new NotImplemented("zzz","InMemoryCNReadCore doesn't implement this method");
    }
    
    
    private void putSystemMetadata(Identifier pid, SystemMetadata smd) {
        this.metaStore.put(pid, smd);
        this.hzSysMetaMap.put(pid,smd);
    }
}
