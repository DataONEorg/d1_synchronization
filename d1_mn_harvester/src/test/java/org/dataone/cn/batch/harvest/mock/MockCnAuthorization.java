/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.harvest.mock;

import org.dataone.service.cn.CoordinatingNodeAuthorization;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AccessPolicy;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;

/**
 *
 * @author waltz
 */
public class MockCnAuthorization implements CoordinatingNodeAuthorization {

    Boolean authorization = true;
    Boolean accessible = true;

    @Override
    public boolean isAuthorized(AuthToken token, Identifier pid, Event operation) throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        return authorization;
    }

    @Override
    public boolean setAccess(AuthToken token, Identifier pid, AccessPolicy accessPolicy) throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        return accessible;
    }

    @Override
    public boolean setAccess(AuthToken token, Identifier pid, String principal, String permission, String permissionType, String permissionOrder) throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        return accessible;
    }

    public Boolean getAccessible() {
        return accessible;
    }

    public void setAccessible(Boolean accessible) {
        this.accessible = accessible;
    }

    public Boolean getAuthorization() {
        return authorization;
    }

    public void setAuthorization(Boolean authorization) {
        this.authorization = authorization;
    }
}
