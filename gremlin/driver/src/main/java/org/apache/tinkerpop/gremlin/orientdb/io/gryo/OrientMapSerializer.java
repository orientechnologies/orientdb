package org.apache.tinkerpop.gremlin.orientdb.io.gryo;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.serializers.MapSerializer;

public class OrientMapSerializer extends MapSerializer {

  MapSerializer mapSerializer = new MapSerializer();

  @Override
  public void write(Kryo kryo, Output output, Map oTrackedMap) {
    LinkedHashMap hashMap = new LinkedHashMap(oTrackedMap);
    mapSerializer.write(kryo, output, hashMap);
  }

  @Override
  protected Map create(Kryo kryo, Input input, Class<Map> type) {
    return kryo.newInstance(LinkedHashMap.class);
  }
}
