package com.orientechnologies.orient.core.metadata.security.jwt;


/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com>
 *         Copyright 2014 Emrul Islam
 */
public interface OJwtHeader {

    public String getAlgorithm();

    public void setAlgorithm(String alg);

    public String getType();

    public void setType(String typ);

    public String getKeyId();

    public void setKeyId(String kid);

}
