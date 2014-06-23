package com.orientechnologies.orient.core.memory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class OMemoryWatchDogTest {

  @Test
  public void handlesListenersWithEqualsOverridenProperly() {

    OMemoryWatchDog oMemoryWatchDog = new OMemoryWatchDog();

    oMemoryWatchDog.addListener(new EqualityListener());
    oMemoryWatchDog.addListener(new EqualityListener());
    oMemoryWatchDog.addListener(new EqualityListener());

    assertEquals(3, oMemoryWatchDog.getListeners().size(), "OMemoryWatchDog should compare listeners by identity");

  }

  @Test
  public void listenersAreFreedByGC() {

    OMemoryWatchDog oMemoryWatchDog = new OMemoryWatchDog();

    for (int i = 0; i < 1000; i++) {
      oMemoryWatchDog.addListener(new EqualityListener());
    }

    OMemoryWatchDog.freeMemoryForResourceCleanup(200);
    assertTrue(oMemoryWatchDog.getListeners().size() < 1000, "OMemoryWatchDog should keep only weak references of listeners");
  }

  // all EqualityListeners are equal
  private class EqualityListener implements OMemoryWatchDog.Listener {
    @Override
    public void lowMemory(long iFreeMemory, long iFreeMemoryPercentage) {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

}
