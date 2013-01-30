package com.orientechnologies.orient.test.internal.index;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.memory.eh.OExtendibleHashingTable;

/**
 * @author Andrey Lomakin
 * @since 30.01.13
 */
public class ExtendibleHashingSpeedTest extends SpeedTestMonoThread {
  private final OExtendibleHashingTable extendibleHashingTable = new OExtendibleHashingTable();
  private final MersenneTwisterFast     mersenneTwisterFast    = new MersenneTwisterFast();

  public ExtendibleHashingSpeedTest() {
    super(10000000);
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    extendibleHashingTable.put(new OPhysicalPosition(new OClusterPositionLong(mersenneTwisterFast.nextLong(Long.MAX_VALUE))));
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    super.deinit();
  }
}
