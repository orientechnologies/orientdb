package com.orientechnologies.orient.stresstest.util;

public class OInitException extends Exception {

    public OInitException(String message) {
        super(OConstants.SYNTAX + "\n" + message);
    }
}
