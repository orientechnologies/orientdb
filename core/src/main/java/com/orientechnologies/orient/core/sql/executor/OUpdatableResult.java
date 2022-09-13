package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Optional;
import java.util.Set;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OUpdatableResult extends OResultInternal {
  protected OResultInternal previousValue = null;
  private final OElement element;

  public OUpdatableResult(OElement element) {
    super(element);
    this.element = element;
  }

  @Override
  public <T> T getProperty(String name) {
    return element.getProperty(name);
  }

  @Override
  public Set<String> getPropertyNames() {
    return element.getPropertyNames();
  }

  public boolean hasProperty(String propName) {
    if (element != null && ((ODocument) element.getRecord()).containsField(propName)) {
      return true;
    }
    return false;
  }

  @Override
  public boolean isElement() {
    return true;
  }

  @Override
  public Optional<OElement> getElement() {
    return Optional.of(element);
  }

  @Override
  public OElement toElement() {
    return element;
  }

  @Override
  public void setProperty(String name, Object value) {
    element.setProperty(name, value);
  }

  public void removeProperty(String name) {
    element.removeProperty(name);
  }
}
