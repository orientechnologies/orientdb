package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OUpdatableResult extends OResultInternal {
  protected OResultInternal previousValue = null;

  public OUpdatableResult(OElement element) {
    super(element);
  }

  @Override
  public boolean isElement() {
    return true;
  }

  public <T> T getProperty(String name) {
    loadElement();
    T result = null;
    if (content != null && content.containsKey(name)) {
      result = (T) content.get(name);
    } else if (isElement()) {
      result = (T) ODocumentInternal.rawPropertyRead((OElement) element, name);
    }
    if (result instanceof OIdentifiable && ((OIdentifiable) result).getIdentity().isPersistent()) {
      result = (T) ((OIdentifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public OElement toElement() {
    return (OElement) element;
  }

  @Override
  public void setProperty(String name, Object value) {
    ((OElement) element).setProperty(name, value);
  }

  public void removeProperty(String name) {
    ((OElement) element).removeProperty(name);
  }
}
