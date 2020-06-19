package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class OrientProperty<V> implements Property<V> {
  protected String key;
  protected V value;
  protected Object wrappedValue;
  protected OrientElement element;

  public OrientProperty(String key, V value, OrientElement element) {
    this.key = key;
    this.value = value;
    this.element = element;
    this.wrappedValue = wrapIntoGraphElement(value);
  }

  private Object wrapIntoGraphElement(V value) {
    Object result = value;
    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }
    if (result instanceof OElement) {
      if (((OElement) result).isVertex()) {
        result =
            element.getGraph().elementFactory().wrapVertex(((OElement) result).asVertex().get());
      } else if (((OElement) value).isEdge()) {
        result = element.getGraph().elementFactory().wrapEdge(((OElement) result).asEdge().get());
      }
    }
    if (result instanceof Collection && containsGraphElements((Collection) result)) {
      if (result instanceof List) {
        result = new VertexEdgeListWrapper((List) result, element);
      } else if (result instanceof Set) {
        result = new VertexEdgeSetWrapper((Set) result, element);
      }
    }
    return result;
  }

  private boolean containsGraphElements(Collection result) {
    for (Object o : result) {
      if (o instanceof OElement) {
        if (((OElement) o).isVertex() || ((OElement) o).isVertex()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public V value() throws NoSuchElementException {
    return (V) wrappedValue;
  }

  @Override
  public boolean isPresent() {
    return value != null;
  }

  @Override
  public Element element() {
    return this.element;
  }

  @Override
  public void remove() {
    OElement doc = element.getRawElement();
    doc.removeProperty(key);
    doc.save();
    this.value = null;
    wrappedValue = null;
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
