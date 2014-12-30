package com.orientechnologies.website.services.reactor;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public enum ReactorMSG {

    ISSUE_IMPORT("issue/import"), INTERNAL_EVENT("internalEvent");

    private String name;

    ReactorMSG(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
