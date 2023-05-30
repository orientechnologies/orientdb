package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition.INDEX_BY;

public class OIndexConfigProperty {
  protected final String name;
  protected final OType type;
  protected final OType linkedType;
  protected final OCollate collate;
  protected final INDEX_BY index_by;

  public OIndexConfigProperty(
      String name, OType type, OType linkedType, OCollate collate, INDEX_BY index_by) {
    this.name = name;
    this.type = type;
    this.linkedType = linkedType;
    this.collate = collate;
    this.index_by = index_by;
  }

  public OCollate getCollate() {
    return collate;
  }

  public OType getLinkedType() {
    return linkedType;
  }

  public String getName() {
    return name;
  }

  public OType getType() {
    return type;
  }

  public INDEX_BY getIndexBy() {
    return index_by;
  }

  public OIndexConfigProperty copy() {
    return new OIndexConfigProperty(
        this.name, this.type, this.linkedType, this.collate, this.index_by);
  }
}
