package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Set;

/**
 * @author Luigi Dell'Aquila
 */
public class OUpdatableResult extends OResultInternal {
  protected OResultInternal previousValue = null;
  private final ODocument element;

  public OUpdatableResult(ODocument element) {
    this.element = element;
  }

  @Override public <T> T getProperty(String name) {
    return element.getProperty(name);
  }

  @Override public Set<String> getPropertyNames() {
    return element.getPropertyNames();
  }

  @Override public boolean isElement() {
    return true;
  }

  @Override public OIdentifiable getElement() {
    return element;
  }

  @Override public OIdentifiable toElement() {
    return element;
  }

  @Override public void setProperty(String name, Object value) {
    element.setProperty(name, value);
  }

  public void removeProperty(String name) {
    element.removeProperty(name);
  }
}
