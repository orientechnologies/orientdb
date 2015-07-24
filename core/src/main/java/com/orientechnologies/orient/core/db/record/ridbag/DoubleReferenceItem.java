package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;

public class DoubleReferenceItem {
  private ORidBag              ridBagOne;
  private OBonsaiBucketPointer oBonsaiBucketPointer;
  private ORidBag              ridBagTwo;
  private String               fieldNameOne;
  private String               fieldNameTwo;

  public DoubleReferenceItem(String fieldNameOne, ORidBag ridBagOne, String fieldNameTwo, ORidBag ridBagTwo,
      OBonsaiBucketPointer oBonsaiBucketPointer) {
    this.ridBagOne = ridBagOne;
    this.oBonsaiBucketPointer = oBonsaiBucketPointer;
    this.ridBagTwo = ridBagTwo;
    this.fieldNameOne = fieldNameOne;
    this.fieldNameTwo = fieldNameTwo;
  }

  public String getFieldNameOne() {
    return fieldNameOne;
  }

  public String getFieldNameTwo() {
    return fieldNameTwo;
  }

  public OBonsaiBucketPointer getoBonsaiBucketPointer() {
    return oBonsaiBucketPointer;
  }

  public ORidBag getRidBagOne() {
    return ridBagOne;
  }

  public ORidBag getRidBagTwo() {
    return ridBagTwo;
  }

}
