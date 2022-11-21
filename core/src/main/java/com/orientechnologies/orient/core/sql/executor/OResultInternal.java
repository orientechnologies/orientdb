package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 06/07/16. */
public class OResultInternal implements OResult {
  protected Map<String, Object> content;
  protected Map<String, Object> temporaryContent;
  protected Map<String, Object> metadata;
  protected OIdentifiable element;

  public OResultInternal() {
    content = new LinkedHashMap<>();
  }

  public OResultInternal(OIdentifiable ident) {
    setElement(ident);
  }

  public void setProperty(String name, Object value) {
    assert element == null;
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (content == null) {
      throw new IllegalStateException("Impossible to mutate result set");
    }
    checkType(value);
    if (value instanceof OResult && ((OResult) value).isElement()) {
      content.put(name, ((OResult) value).getElement().get());
    } else {
      content.put(name, value);
    }
  }

  private void checkType(Object value) {
    if (value == null) {
      return;
    }
    if (OType.isSimpleType(value) || value instanceof Character) {
      return;
    }
    if (value instanceof OIdentifiable) {
      return;
    }
    if (value instanceof OResult) {
      return;
    }
    if (value instanceof Collection || value instanceof Map) {
      return;
    }
    if (value instanceof OSerializableStream || value instanceof Serializable) {
      return;
    }
    throw new IllegalArgumentException(
        "Invalid property value for OResult: " + value + " - " + value.getClass().getName());
  }

  public void setTemporaryProperty(String name, Object value) {
    if (temporaryContent == null) {
      temporaryContent = new HashMap<>();
    }
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (value instanceof OResult && ((OResult) value).isElement()) {
      temporaryContent.put(name, ((OResult) value).getElement().get());
    } else {
      temporaryContent.put(name, value);
    }
  }

  public Object getTemporaryProperty(String name) {
    if (name == null || temporaryContent == null) {
      return null;
    }
    return temporaryContent.get(name);
  }

  public Set<String> getTemporaryProperties() {
    return temporaryContent == null ? Collections.emptySet() : temporaryContent.keySet();
  }

  public void removeProperty(String name) {
    if (content != null) {
      content.remove(name);
    }
  }

  public <T> T getProperty(String name) {
    T result = null;
    if (content != null && content.containsKey(name)) {
      result = (T) wrap(content.get(name));
    } else if (element != null) {
      result = (T) wrap(((ODocument) element.getRecord()).getProperty(name));
    }
    if (result instanceof OIdentifiable && ((OIdentifiable) result).getIdentity().isPersistent()) {
      result = (T) ((OIdentifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public OElement getElementProperty(String name) {
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = ((ODocument) element.getRecord()).getProperty(name);
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? (OElement) result : null;
  }

  @Override
  public OVertex getVertexProperty(String name) {
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = ((ODocument) element.getRecord()).getProperty(name);
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? ((OElement) result).asVertex().orElse(null) : null;
  }

  @Override
  public OEdge getEdgeProperty(String name) {
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = ((ODocument) element.getRecord()).getProperty(name);
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? ((OElement) result).asEdge().orElse(null) : null;
  }

  @Override
  public OBlob getBlobProperty(String name) {
    Object result = null;
    if (content != null && content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = ((ODocument) element.getRecord()).getProperty(name);
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OBlob ? (OBlob) result : null;
  }

  private Object wrap(Object input) {
    if (input instanceof OElement && !((OElement) input).getIdentity().isValid()) {
      OResultInternal result = new OResultInternal();
      OElement elem = (OElement) input;
      for (String prop : elem.getPropertyNames()) {
        result.setProperty(prop, elem.getProperty(prop));
      }
      elem.getSchemaType().ifPresent(x -> result.setProperty("@class", x.getName()));
      return result;
    } else if (isEmbeddedList(input)) {
      return ((List) input).stream().map(this::wrap).collect(Collectors.toList());
    } else if (isEmbeddedSet(input)) {
      return ((Set) input).stream().map(this::wrap).collect(Collectors.toSet());
    } else if (isEmbeddedMap(input)) {
      Map result = new HashMap();
      for (Map.Entry<Object, Object> o : ((Map<Object, Object>) input).entrySet()) {
        result.put(o.getKey(), wrap(o.getValue()));
      }
      return result;
    }
    return input;
  }

  private boolean isEmbeddedSet(Object input) {
    return OType.getTypeByValue(input) == OType.EMBEDDEDSET && input instanceof Set;
  }

  private boolean isEmbeddedMap(Object input) {
    return OType.getTypeByValue(input) == OType.EMBEDDEDMAP && input instanceof Map;
  }

  private boolean isEmbeddedList(Object input) {
    return OType.getTypeByValue(input) == OType.EMBEDDEDLIST && input instanceof List;
  }

  public Set<String> getPropertyNames() {
    if (element != null && !(element instanceof ORecordBytes)) {
      return ((ODocument) element.getRecord()).getPropertyNames();
    } else if (content != null) {
      return new LinkedHashSet<>(content.keySet());
    } else {
      return Collections.emptySet();
    }
  }

  public boolean hasProperty(String propName) {
    if (element != null && ((ODocument) element.getRecord()).containsField(propName)) {
      return true;
    }
    if (content != null) {
      return content.containsKey(propName);
    }
    return false;
  }

  @Override
  public boolean isElement() {
    if (element == null) {
      return false;
    }
    if (element instanceof OElement) {
      return true;
    }
    if (element.getRecord() instanceof OElement) {
      return true;
    }
    return false;
  }

  public Optional<OElement> getElement() {
    if (element == null || element instanceof OElement) {
      return Optional.ofNullable((OElement) element);
    }
    ORecord rec = element.getRecord();
    if (rec instanceof OElement) {
      return Optional.ofNullable((OElement) rec);
    }
    return Optional.empty();
  }

  @Override
  public OElement toElement() {
    if (isElement()) {
      return getElement().get();
    }
    ODocument doc = new ODocument();
    for (String s : getPropertyNames()) {
      if (s == null) {
        continue;
      } else if (s.equalsIgnoreCase("@rid")) {
        Object newRid = getProperty(s);
        if (newRid instanceof OIdentifiable) {
          newRid = ((OIdentifiable) newRid).getIdentity();
        } else {
          continue;
        }
        ORecordId oldId = (ORecordId) doc.getIdentity();
        oldId.setClusterId(((ORID) newRid).getClusterId());
        oldId.setClusterPosition(((ORID) newRid).getClusterPosition());
      } else if (s.equalsIgnoreCase("@version")) {
        Object v = getProperty(s);
        if (v instanceof Number) {
          ORecordInternal.setVersion(doc, ((Number) v).intValue());
        } else {
          continue;
        }
      } else if (s.equalsIgnoreCase("@class")) {
        doc.setClassName(getProperty(s));
      } else {
        doc.setProperty(s, convertToElement(getProperty(s)));
      }
    }
    return doc;
  }

  @Override
  public Optional<ORID> getIdentity() {
    if (element != null) {
      return Optional.of(element.getIdentity());
    }
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return this.element == null;
  }

  @Override
  public Optional<ORecord> getRecord() {
    if (this.element == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(this.element.getRecord());
  }

  @Override
  public boolean isBlob() {
    return this.element != null && this.element.getRecord() instanceof OBlob;
  }

  @Override
  public Optional<OBlob> getBlob() {
    if (isBlob()) {
      return Optional.ofNullable(this.element.getRecord());
    }
    return Optional.empty();
  }

  @Override
  public Object getMetadata(String key) {
    if (key == null) {
      return null;
    }
    return metadata == null ? null : metadata.get(key);
  }

  public void setMetadata(String key, Object value) {
    if (key == null) {
      return;
    }
    if (metadata == null) {
      metadata = new HashMap<>();
    }
    metadata.put(key, value);
  }

  public void clearMetadata() {
    metadata = null;
  }

  public void removeMetadata(String key) {
    if (key == null || metadata == null) {
      return;
    }
    metadata.remove(key);
  }

  public void addMetadata(Map<String, Object> values) {
    if (values == null) {
      return;
    }
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.putAll(values);
  }

  @Override
  public Set<String> getMetadataKeys() {
    return metadata == null ? Collections.emptySet() : metadata.keySet();
  }

  private Object convertToElement(Object property) {
    if (property instanceof OResult) {
      return ((OResult) property).toElement();
    }
    if (property instanceof List) {
      return ((List) property).stream().map(x -> convertToElement(x)).collect(Collectors.toList());
    }

    if (property instanceof Set) {
      return ((Set) property).stream().map(x -> convertToElement(x)).collect(Collectors.toSet());
    }

    if (property instanceof Map) {
      Map<Object, Object> result = new HashMap<>();
      Map<Object, Object> prop = ((Map) property);
      for (Map.Entry<Object, Object> o : prop.entrySet()) {
        result.put(o.getKey(), convertToElement(o.getValue()));
      }
    }

    return property;
  }

  public void setElement(OIdentifiable element) {
    if (element instanceof OElement) {
      this.element = element;
    } else {
      this.element = element.getRecord();
    }
    if (element instanceof OContextualRecordId) {
      this.addMetadata(((OContextualRecordId) element).getContext());
    }
    this.content = null;
  }

  @Override
  public String toString() {
    if (element != null) {
      return element.toString();
    }
    return "{\n"
        + content.entrySet().stream()
            .map(x -> x.getKey() + ": " + x.getValue())
            .reduce("", (a, b) -> a + b + "\n")
        + "}\n";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OResultInternal)) {
      return false;
    }
    OResultInternal resultObj = (OResultInternal) obj;
    if (element != null) {
      if (!resultObj.getElement().isPresent()) {
        return false;
      }
      return element.equals(resultObj.getElement().get());
    } else {
      if (resultObj.getElement().isPresent()) {
        return false;
      }
      if (content != null) {
        return this.content.equals(resultObj.content);
      } else {
        return resultObj.content == null;
      }
    }
  }

  @Override
  public int hashCode() {
    if (element != null) {
      return element.hashCode();
    }
    if (content != null) {
      return content.hashCode();
    } else {
      return super.hashCode();
    }
  }

  public void bindToCache(ODatabaseDocumentInternal db) {
    if (isRecord()) {
      ORecord rec = element.getRecord();
      ORecord cached = db.getLocalCache().findRecord(rec.getIdentity());
      if (cached != null) {
        if (!cached.isDirty()) {
          cached.fromStream(rec.toStream());
        }
        element = cached;
      } else {
        db.getLocalCache().updateRecord(rec);
      }
    }
  }
}
