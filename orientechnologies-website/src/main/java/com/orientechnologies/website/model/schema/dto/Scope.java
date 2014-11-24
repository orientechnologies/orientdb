package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 24/11/14.
 */
public class Scope {

  protected String     id;
  protected String     name;
  protected Integer    number;
  protected Repository repository;

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
}
