package com.orientechnologies.orient.core.index.comparator;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Comparator;

public class DescComparator implements Comparator<ORawPair<Object, ORID>> {
  public static final DescComparator INSTANCE = new DescComparator();

  @Override
  public int compare(ORawPair<Object, ORID> entryOne, ORawPair<Object, ORID> entryTwo) {
    return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
  }
}
