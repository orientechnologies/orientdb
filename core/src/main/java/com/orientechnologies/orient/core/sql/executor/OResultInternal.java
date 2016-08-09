package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OResultInternal implements OResult {
  protected Map<String, Object> content = new HashMap<>();
  protected OIdentifiable element;

  public void setProperty(String name, Object value) {
    content.put(name, value);
  }

  public <T> T getProperty(String name) {
    if (content.containsKey(name)) {
      return (T) content.get(name);
    }
    if (element != null) {
      return ((ODocument) element.getRecord()).getProperty(name);
    }
    return null;
  }

  public Set<String> getPropertyNames() {
    Set<String> result = new HashSet<>();
    if (element != null) {
      result.addAll(((ODocument) element.getRecord()).getPropertyNames());
    }
    result.addAll(content.keySet());
    return result;
  }

  @Override public boolean isElement() {
    return this.element != null;
  }

  public OIdentifiable getElement() {
    return element;
  }

  @Override public OIdentifiable toElement() {
    throw new UnsupportedOperationException("Implement OResultInternal.toElement()!");
  }

  public void setElement(OIdentifiable element) {
    this.element = element;
  }

  @Override public String toString() {
    if (getElement() != null) {
      return getElement().toString();
    }
    return "{\n" +
        content.entrySet().stream().map(x -> x.getKey() + ": \n" + x.getValue()).reduce("", (a, b) -> a + b + "\n\n") + "}\n";
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OResultInternal)) {
      return false;
    }
    OResultInternal resultObj = (OResultInternal) obj;
    if (getElement() != null) {
      if (resultObj.getElement() == null) {
        return false;
      }
      return getElement().equals(resultObj.getElement());
    } else {
      if (resultObj.getElement() != null) {
        return false;
      }
      return this.content.equals(resultObj.content);
    }
  }

  @Override public int hashCode() {
    if (getElement() != null) {
      return getElement().hashCode();
    }
    return content.hashCode();
  }
}
