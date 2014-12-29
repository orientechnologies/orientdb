package com.orientechnologies.website.model.schema.dto.web.hateoas;

import java.util.List;

/**
 * Created by Enrico Risa on 29/12/14.
 */
public class ScopeDTO {

    protected String name;
    protected Integer number;
    protected String owner;
    protected String repository;
    protected List<String> members;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getRepository() {
        return repository;
    }

    public void setRepository(String repository) {
        this.repository = repository;
    }

    public List<String> getMembers() {
        return members;
    }

    public void setMembers(List<String> members) {
        this.members = members;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }
}
