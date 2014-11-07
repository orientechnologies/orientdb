package com.emrul.orient.jwt.impl;

import com.orientechnologies.orient.core.metadata.security.jwt.IJwtHeader;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com>
 *         Copyright 2014 Emrul Islam
 */
public class JwtHeader implements IJwtHeader {

    private String typ;
    private String alg;
    private String kid;

    @Override
    public String getAlggorithm() {
        return alg;
    }

    @Override
    public void setAlgorithm(String alg) {
        this.alg = alg;
    }

    @Override
    public String getType() {
        return typ;
    }

    @Override
    public void setType(String typ) {
        this.typ = typ;
    }

    @Override
    public String getKeyId() {
        return kid;
    }

    @Override
    public void setKeyId(String kid) {
        this.kid = kid;
    }

}
