package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class ODocumentEntry {

  public Object                                          value;
  public Object                                          original;
  public OType                                           type;
  public OProperty                                       property;
  public OSimpleMultiValueChangeListener<Object, Object> changeListener;
  public OMultiValueChangeTimeLine<Object, Object>       timeLine;
  public boolean                                         changed = false;
  public boolean                                         exist   = true;
  public boolean                                         created = false;

  public ODocumentEntry() {

  }

  protected ODocumentEntry clone() {
    ODocumentEntry entry = new ODocumentEntry();
    entry.type = type;
    entry.property = property;
    entry.value = value;
    entry.changed = changed;
    entry.created = created;
    entry.exist = exist;
    return entry;
  }

}
