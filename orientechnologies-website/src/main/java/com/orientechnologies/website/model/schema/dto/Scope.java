package com.orientechnologies.website.model.schema.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Created by Enrico Risa on 24/11/14.
 */
public class Scope {

  @JsonIgnore
  protected String      id;
  protected String      name;
  protected Integer     number;
  @JsonIgnore
  protected OUser       owner;
  @JsonIgnore
  protected List<OUser> members;
  protected Repository  repository;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Repository getRepository() {
    return repository;
  }

  public void setRepository(Repository repository) {
    this.repository = repository;
  }

  public Integer getNumber() {
    return number;
  }

  public void setNumber(Integer number) {
    this.number = number;
  }

  public OUser getOwner() {
    return owner;
  }

  public void setOwner(OUser owner) {
    this.owner = owner;
  }

  public List<OUser> getMembers() {
    return members;
  }

  public void setMembers(List<OUser> members) {
    this.members = members;
  }
}
