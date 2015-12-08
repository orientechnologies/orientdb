package com.orientechnologies.orient.test.internal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
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

    directMemoryPointer = ODirectMemoryPointerFactory.instance()
        .createPointer(OBinarySerializerFactory.getInstance().getObjectSerializer(OType.STRING)
        .getObjectSize(longString));
  }

  @Test(enabled = false)
  @Override
  public void cycle() throws Exception {
    stringSerializer.serializeInDirectMemoryObject(longString, directMemoryPointer, 0);
    stringSerializer.deserializeFromDirectMemoryObject(directMemoryPointer, 0);
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    super.deinit();

    directMemoryPointer.free();
  }
}
