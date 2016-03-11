package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;

/**
 * Created by tglman on 11/03/16.
 */
public class ONestedMultiValueChangeEvent<K, V> extends OMultiValueChangeEvent<K, V> {

  private OMultiValueChangeTimeLine timeLine;

  public ONestedMultiValueChangeEvent(K key, V value) {
    super(OChangeType.NESTED, key, value);
  }

  public ONestedMultiValueChangeEvent(K key, V value, V oldValue) {
    super(OChangeType.NESTED, key, value, oldValue);
  }

  public ONestedMultiValueChangeEvent(K key, V value, V oldValue, boolean changesOwnerContent) {
    super(OChangeType.NESTED, key, value, oldValue, changesOwnerContent);
  }

  public OMultiValueChangeTimeLine getTimeLine() {
    return timeLine;
  }

  public void setTimeLine(OMultiValueChangeTimeLine timeLine) {
    this.timeLine = timeLine;
  }
}
