package com.orientechnologies.website.model.schema.dto.web;

import java.util.List;

/**
 * Created by Enrico Risa on 07/01/15.
 */
public class ImportDTO {

    protected String state;
    protected List<Integer> issues;

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<Integer> getIssues() {
        return issues;
    }

    public void setIssues(List<Integer> issues) {
        this.issues = issues;
    }
}
