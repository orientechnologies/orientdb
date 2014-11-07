package com.emrul.orient.jwt.impl;


import com.orientechnologies.orient.core.db.record.ODatabaseRecordInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.IToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.jwt.IJsonWebToken;
import com.orientechnologies.orient.core.metadata.security.jwt.IJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.IJwtPayload;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com>
 *         Copyright 2014 Emrul Islam
 */
public class JsonWebToken implements IJsonWebToken, IToken {

    public final IJwtHeader header;
    public final IJwtPayload payload;
    private boolean isVerified;
    private boolean isValid;

    public JsonWebToken() {
        this(new JwtHeader(), new OrientJwtPayload());
    }

    public JsonWebToken(IJwtHeader header, IJwtPayload payload) {
        isVerified = false;
        isValid = false;
        this.header = header;
        this.payload = payload;
    }

    @Override
    public IJwtHeader getHeader() {
        return header;
    }

    @Override
    public IJwtPayload getPayload() {
        return payload;
    }

    @Override
    public boolean getIsVerified() {
        return isVerified;
    }

    @Override
    public void setIsVerified(boolean verified) {
        this.isVerified = verified;
    }

    @Override
    public boolean getIsValid() {
        return this.isValid;
    }

    @Override
    public void setIsValid(boolean valid) {
        this.isValid = valid;
    }

    @Override
    public String getSubject() {
        return payload.getSubject();
    }

    @Override
    public OUser getUser(ODatabaseRecordInternal db) {
        String userRid = ((OrientJwtPayload)payload).getUserRid();
        ODocument result;
        result = db.load(new ORecordId(userRid), "roles:1");
        if (!result.getClassName().equals(OUser.CLASS_NAME)) {
          result = null;
        }
        return new OUser(result);

    }
}
