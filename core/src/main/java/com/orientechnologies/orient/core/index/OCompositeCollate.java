package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.collate.OCollate;

public class OCompositeCollate implements OCollate {
  /**
   * 
   */
  private final OAbstractIndexDefinition oCompositeIndexDefinition;

  /**
   * @param oCompositeIndexDefinition
   */
  public OCompositeCollate(OAbstractIndexDefinition oCompositeIndexDefinition) {
    this.oCompositeIndexDefinition = oCompositeIndexDefinition;
  }

  private final List<OCollate> collates = new ArrayList<OCollate>();

  public void addCollate(OCollate collate) {
    collates.add(collate);
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException("getName");
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object transform(Object obj) {
    List<Object> keys = null;
    if (obj instanceof OCompositeKey) {
      final OCompositeKey compositeKey = (OCompositeKey) obj;
      keys = compositeKey.getKeys();
    } else if (obj instanceof List) {
      keys = (List<Object>) obj;
    } else {
      throw new OIndexException("Impossible add as key of a CompositeIndex a value of type " + obj.getClass());
    }

    OCompositeKey transformedKey = new OCompositeKey();

    final int size = Math.min(keys.size(), collates.size());
    for (int i = 0; i < size; i++) {
      final Object key = keys.get(i);

      final OCollate collate = collates.get(i);
      transformedKey.addKey(collate.transform(key));
    }

    for (int i = size; i < keys.size(); i++)
      transformedKey.addKey(keys.get(i));

    return transformedKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OCompositeCollate that = (OCompositeCollate) o;

    if (!collates.equals(that.collates))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return collates.hashCode();
  }

  public List<OCollate> getCollates() {
    return collates;
  }

  @Override
  public String toString() {
    return "OCompositeCollate{" + "collates=" + collates + ", null values ignored = "
        + this.oCompositeIndexDefinition.isNullValuesIgnored() + '}';
  }
}