package com.orientechnologies.orient.test.domain.base;

import javax.persistence.Id;
import javax.persistence.Version;

public class IdObject implements Comparable<IdObject> {

  public IdObject() {
    super();
  }

  @Id private String id;
  @Version private Integer version;

  public static boolean isUnidentified(IdObject anObject) {
    boolean response = (anObject.id == null);
    return response;
  }

  public int compareTo(IdObject another) {
    return this.id.compareTo(another.id);
  }

  @Override
  public boolean equals(Object another) {
    return this.id.equals(((IdObject) another).id);
  }

  public String getId() {
    return id;
  }

  public String getKey() {
    return this.getId();
  }

  public void setId(String anId) {
    this.id = anId;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }
}
