package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 04/12/14.
 */
public class Environment {

  protected String    id;
  protected Integer   eid;
  protected String    name;
  protected Milestone version;
  protected String    description;
  protected String    os;
  protected Integer ram;
  protected String    jvm;
  protected String    connectionType;
  protected boolean   distributed;

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

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Integer getEid() {
    return eid;
  }

  public void setEid(Integer eid) {
    this.eid = eid;
  }

  public Milestone getVersion() {
    return version;
  }

  public void setVersion(Milestone version) {
    this.version = version;
  }

  public String getOs() {
    return os;
  }

  public void setOs(String os) {
    this.os = os;
  }

  public Integer getRam() {
    return ram;
  }

  public void setRam(Integer ram) {
    this.ram = ram;
  }

  public String getJvm() {
    return jvm;
  }

  public void setJvm(String jvm) {
    this.jvm = jvm;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public void setConnectionType(String connectionType) {
    this.connectionType = connectionType;
  }

  public boolean isDistributed() {
    return distributed;
  }

  public void setDistributed(boolean distributed) {
    this.distributed = distributed;
  }
}
