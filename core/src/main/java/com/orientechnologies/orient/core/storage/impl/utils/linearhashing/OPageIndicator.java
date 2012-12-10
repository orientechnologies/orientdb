package com.orientechnologies.orient.core.storage.impl.utils.linearhashing;

import java.util.BitSet;

/**
 * @author Artem Loginov (logart2007@gmail.com)
 */
public class OPageIndicator {
  private BitSet indicator = new BitSet();

  public boolean get(int i) {
    return indicator.get(i);
  }

  public int getRealPosInSecondaryIndex(int page) {
    if (page < 0) {
      throw new RuntimeException("page number in page indicator should be positive");
    }
    if (!indicator.get(page)) {
      throw new RuntimeException("page which accessed should be used before");
    }

    return indicator.get(0, page).cardinality();
  }

  public void set(int pageToUse) {
    assert !indicator.get(pageToUse);
    indicator.set(pageToUse);
  }

  public int getFirstEmptyPage(int startingPage, int size) {
    int emptyPage = indicator.nextClearBit(startingPage);
    return emptyPage < startingPage + size ? emptyPage : -1;
  }

  public void unset(int pageToUse) {
    assert indicator.get(pageToUse);
    indicator.set(pageToUse, false);
  }

  @Override
  public String toString() {
    return indicator.toString();
  }

  public void clear() {
    indicator.clear();
  }

  public boolean isUsedPageExistInRange(int startPosition, int endPosition) {
    return indicator.get(startPosition, endPosition).cardinality()>0;
  }
}
