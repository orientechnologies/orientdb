package com.orientechnologies.orient.core.tx;

import java.util.List;
import java.util.Optional;

import com.orientechnologies.orient.core.metadata.schema.OClass;

public class OIndexOperation {

  public enum Type {
    CREATE,
    DROP,
  }

  private Type type;
  private String name;
  private Optional<OClass.INDEX_TYPE> indexType;
  private Optional<String> className;
  private List<String> fields;
  private Optional<String> collate;

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Optional<OClass.INDEX_TYPE> getIndexType() {
    return indexType;
  }

  public void setIndexType(Optional<OClass.INDEX_TYPE> indexType) {
    this.indexType = indexType;
  }

  public Optional<String> getClassName() {
    return className;
  }

  public void setClassName(Optional<String> className) {
    this.className = className;
  }

  public List<String> getFields() {
    return fields;
  }

  public void setFields(List<String> fields) {
    this.fields = fields;
  }

  public Optional<String> getCollate() {
    return collate;
  }

  public void setCollate(Optional<String> collate) {
    this.collate = collate;
  }
}
