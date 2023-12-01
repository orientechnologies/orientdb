package com.orientechnologies.orient.core.index.comparator;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Comparator;

public class AscComparator implements Comparator<ORawPair<Object, ORID>> {
  public static final AscComparator INSTANCE = new AscComparator();

  @Override
  public int compare(ORawPair<Object, ORID> entryOne, ORawPair<Object, ORID> entryTwo) {
    return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
  }
}
