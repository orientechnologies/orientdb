package com.orientechnologies.orient.test.internal;

import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.test.SpeedTestMonoThread;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 11/7/13
 */
public class StringSerializerSpeedTest extends SpeedTestMonoThread {
  private final OStringSerializer stringSerializer = new OStringSerializer();
  private final String            longString       = "Alice : If it had grown up, it would have made a dreadfully ugly child; but it makes rather a handsome pig, I think";
  private ODirectMemoryPointer    directMemoryPointer;

  public StringSerializerSpeedTest() {
    super(1000000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    super.init();

    directMemoryPointer = new ODirectMemoryPointer(OStringSerializer.INSTANCE.getObjectSize(longString));
  }

  @Test(enabled = false)
  @Override
  public void cycle() throws Exception {
    stringSerializer.serializeInDirectMemory(longString, directMemoryPointer, 0);
    stringSerializer.deserializeFromDirectMemory(directMemoryPointer, 0);
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    super.deinit();

    directMemoryPointer.free();
  }
}
