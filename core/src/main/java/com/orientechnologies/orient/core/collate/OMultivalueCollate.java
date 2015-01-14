package com.orientechnologies.orient.core.collate;

import java.util.List;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.orient.core.index.OCompositeKey;

public class OMultivalueCollate extends ODefaultComparator implements OCollate {

  public static final String NAME = "multivalue";

  public String getName() {
    return NAME;
  }

  public Object transform(final Object obj) {
    if (obj instanceof List<?>)
      return new OCompositeKey((List<?>) obj);
    return obj;
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass())
      return false;

    final ODefaultCollate that = (ODefaultCollate) obj;

    return getName().equals(that.getName());
  }

  @Override
  public String toString() {
    return "{" + getClass().getSimpleName() + " : name = " + getName() + "}";
  }
}
