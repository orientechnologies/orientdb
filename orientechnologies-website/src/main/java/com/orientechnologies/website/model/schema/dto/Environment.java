package com.orientechnologies.website.model.schema.dto;

/**
 * Created by Enrico Risa on 04/12/14.
 */
public class Environment {

  protected String    id;
  protected Long      eid;
  protected String    name;
  protected Milestone version;
  protected Integer   versionNumber;
  protected String    repoName;
  protected String    description;
  protected String    os;
  protected String    ram;
  protected String    jvm;
  protected String    connectionType;
  protected Boolean   distributed;
  protected String    note;

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

  public String getRepoName() {
    return repoName;
  }

  public void setRepoName(String repoName) {
    this.repoName = repoName;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Long getEid() {
    return eid;
  }

  public void setEid(Long eid) {
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

  public String getRam() {
    return ram;
  }

  public void setRam(String ram) {
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

  public Boolean getDistributed() {
    return distributed;
  }

  public void setDistributed(boolean distributed) {
    this.distributed = distributed;
  }

  public Integer getVersionNumber() {
    return versionNumber == null ? (getVersion() != null ? getVersion().getNumber() : null) : versionNumber;
  }

  public void setVersionNumber(Integer versionNumber) {
    this.versionNumber = versionNumber;
  }

  public String getNote() {
    return note;
  }

  public void setNote(String note) {
    this.note = note;
  }
}
