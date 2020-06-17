package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.lang.reflect.Array;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Created by luigidellaquila on 21/07/16. */
public interface OResult {

  /**
   * returns a property from the result
   *
   * @param name the property name
   * @param <T>
   * @return the property value. If the property value is a persistent record, it only returns the
   *     RID. See also {@link #getElementProperty(String)} {@link #getVertexProperty(String)} {@link
   *     #getEdgeProperty(String)} {@link #getBlobProperty(String)}
   */
  <T> T getProperty(String name);

  /**
   * returns an OElement property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an OElement
   */
  OElement getElementProperty(String name);

  /**
   * returns an OVertex property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an OVertex
   */
  OVertex getVertexProperty(String name);

  /**
   * returns an OEdge property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an OEdge
   */
  OEdge getEdgeProperty(String name);

  /**
   * returns an OEdge property from the result
   *
   * @param name the property name
   * @return the property value. Null if the property is not defined or if it's not an OEdge
   */
  OBlob getBlobProperty(String name);

  Set<String> getPropertyNames();

  Optional<ORID> getIdentity();

  boolean isElement();

  Optional<OElement> getElement();

  OElement toElement();

  default boolean isVertex() {
    return getElement().map(x -> x.isVertex()).orElse(false);
  }

  default Optional<OVertex> getVertex() {
    return getElement().flatMap(x -> x.asVertex());
  }

  default boolean isEdge() {
    return getElement().map(x -> x.isEdge()).orElse(false);
  }

  default Optional<OEdge> getEdge() {
    return getElement().flatMap(x -> x.asEdge());
  }

  boolean isBlob();

  Optional<OBlob> getBlob();

  Optional<ORecord> getRecord();

  default boolean isRecord() {
    return !isProjection();
  }

  boolean isProjection();

  /**
   * return metadata related to current result given a key
   *
   * @param key the metadata key
   * @return metadata related to current result given a key
   */
  Object getMetadata(String key);

  /**
   * return all the metadata keys available
   *
   * @return all the metadata keys available
   */
  Set<String> getMetadataKeys();

  default String toJSON() {
    if (isElement()) {
      return getElement().get().toJSON();
    }
    StringBuilder result = new StringBuilder();
    result.append("{");
    boolean first = true;
    for (String prop : getPropertyNames()) {
      if (!first) {
        result.append(", ");
      }
      result.append(toJson(prop));
      result.append(": ");
      result.append(toJson(getProperty(prop)));
      first = false;
    }
    result.append("}");
    return result.toString();
  }

  default String toJson(Object val) {
    String jsonVal = null;
    if (val == null) {
      jsonVal = "null";
    } else if (val instanceof String) {
      jsonVal = "\"" + encode(val.toString()) + "\"";
    } else if (val instanceof Number || val instanceof Boolean) {
      jsonVal = val.toString();
    } else if (val instanceof OResult) {
      jsonVal = ((OResult) val).toJSON();
    } else if (val instanceof OElement) {
      ORID id = ((OElement) val).getIdentity();
      if (id.isPersistent()) {
        //        jsonVal = "{\"@rid\":\"" + id + "\"}"; //TODO enable this syntax when Studio and
        // the parsing are OK
        jsonVal = "\"" + id + "\"";
      } else {
        jsonVal = ((OElement) val).toJSON();
      }
    } else if (val instanceof ORID) {
      //      jsonVal = "{\"@rid\":\"" + val + "\"}"; //TODO enable this syntax when Studio and the
      // parsing are OK
      jsonVal = "\"" + val + "\"";
    } else if (val instanceof Iterable) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      Iterator iterator = ((Iterable) val).iterator();
      while (iterator.hasNext()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(iterator.next()));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Iterator) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      Iterator iterator = (Iterator) val;
      while (iterator.hasNext()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(iterator.next()));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Map) {
      StringBuilder builder = new StringBuilder();
      builder.append("{");
      boolean first = true;
      Map<Object, Object> map = (Map) val;
      for (Map.Entry entry : map.entrySet()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(entry.getKey()));
        builder.append(": ");
        builder.append(toJson(entry.getValue()));
        first = false;
      }
      builder.append("}");
      jsonVal = builder.toString();
    } else if (val instanceof byte[]) {
      jsonVal = "\"" + Base64.getEncoder().encodeToString((byte[]) val) + "\"";
    } else if (val instanceof Date) {
      jsonVal = "\"" + ODateHelper.getDateTimeFormatInstance().format(val) + "\"";
    } else if (val.getClass().isArray()) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      for (int i = 0; i < Array.getLength(val); i++) {
        if (i > 0) {
          builder.append(", ");
        }
        builder.append(toJson(Array.get(val, i)));
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else {
      throw new UnsupportedOperationException(
          "Cannot convert " + val + " - " + val.getClass() + " to JSON");
    }
    return jsonVal;
  }

  default String encode(String s) {
    String result = s.replaceAll("\\\\", "\\\\\\\\");
    result = result.replaceAll("\"", "\\\\\"");
    result = result.replaceAll("\n", "\\\\n");
    result = result.replaceAll("\t", "\\\\t");
    result = result.replaceAll("\r", "\\\\r");
    return result;
  }

  boolean hasProperty(String varName);
}
