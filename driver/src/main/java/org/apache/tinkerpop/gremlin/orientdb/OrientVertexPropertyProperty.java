package org.apache.tinkerpop.gremlin.orientdb;

import java.util.NoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class OrientVertexPropertyProperty<U> implements Property<U> {

  private final String key;
  private final U value;
  private final OrientVertexProperty<?> source;

  public OrientVertexPropertyProperty(
      String key, U value, OrientVertexProperty<?> orientVertexProperty) {
    this.key = key;
    this.value = value;
    this.source = orientVertexProperty;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public U value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return value != null;
  }

  @Override
  public Element element() {
    return source;
  }

  @Override
  public void remove() {
    source.removeMetadata(key);
  }

  @Override
  public String toString() {
    return StringFactory.propertyString(this);
  }

  @Override
  public final boolean equals(final Object object) {
    return ElementHelper.areEqual(this, object);
  }

  @Override
  public int hashCode() {
    return ElementHelper.hashCode(this);
  }
}
