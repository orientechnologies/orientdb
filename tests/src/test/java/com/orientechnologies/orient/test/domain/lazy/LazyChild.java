package com.orientechnologies.orient.test.domain.lazy;

import com.orientechnologies.orient.core.id.ORID;
import javax.persistence.Id;

/** @author Wouter de Vaal */
public class LazyChild {

  @Id private ORID id;

  private String name;

  public ORID getId() {
    return id;
  }

  public void setId(ORID id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
