package com.orientechnologies.orient.core.serialization.serializer.result.binary;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/** Created by tglman on 25/05/16. */
public class OResultSerializerNetworkTest {

  @Test
  public void test() {
    OResultSerializerNetwork serializer = new OResultSerializerNetwork();

    OResultInternal original = new OResultInternal();
    original.setProperty("string", "foo");
    original.setProperty("integer", 12);
    original.setProperty("float", 12.4f);
    original.setProperty("double", 12.4d);
    original.setProperty("boolean", true);
    original.setProperty("rid", new ORecordId("#12:0"));

    OResultInternal embeddedProj = new OResultInternal();
    embeddedProj.setProperty("name", "bar");
    original.setProperty("embeddedProj", embeddedProj);

    List list = new ArrayList();
    list.add("foo");
    list.add("bar");
    original.setProperty("list", list);

    Set set = new HashSet<>();
    set.add("foox");
    set.add("barx");
    original.setProperty("set", "set");

    BytesContainer bytes = new BytesContainer();
    serializer.serialize(original, bytes);

    bytes.offset = 0;
    OResultInternal deserialized = serializer.deserialize(bytes);
    Assert.assertEquals(original, deserialized);
  }
}
