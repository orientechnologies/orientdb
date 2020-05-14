package org.apache.tinkerpop.gremlin.orientdb.io;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import org.apache.tinkerpop.gremlin.orientdb.io.graphson.OrientGraphSONV3;
import org.apache.tinkerpop.gremlin.orientdb.io.kryo.ORecordIdKryoSerializer;
import org.apache.tinkerpop.gremlin.orientdb.io.kryo.ORidBagKryoSerializer;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import java.util.Map;

@SuppressWarnings("serial")
public class OrientIoRegistry extends AbstractIoRegistry {

  public static final String CLUSTER_ID       = "clusterId";
  public static final String CLUSTER_POSITION = "clusterPosition";

  private static final OrientIoRegistry INSTANCE = new OrientIoRegistry();

  private OrientIoRegistry() {
    register(GryoIo.class, ORecordId.class, new ORecordIdKryoSerializer());
    register(GryoIo.class, ORidBag.class, new ORidBagKryoSerializer());

    register(GraphSONIo.class, ORecordId.class, OrientGraphSONV3.INSTANCE);
  }

  public static OrientIoRegistry instance() {
    return INSTANCE;
  }

  public static OrientIoRegistry getInstance() {
    return INSTANCE;
  }

  public static ORecordId newORecordId(final Object obj) {
    if (obj == null) {
      return null;
    }

    if (obj instanceof ORecordId) {
      return (ORecordId) obj;
    }

    if (obj instanceof Map) {
      @SuppressWarnings({ "unchecked", "rawtypes" })
      final Map<String, Number> map = (Map) obj;
      return new ORecordId(map.get(CLUSTER_ID).intValue(), map.get(CLUSTER_POSITION).longValue());
    }
    throw new IllegalArgumentException("Unable to convert unknow type to ORecordId " + obj.getClass());
  }

  public static boolean isORecord(final Object result) {
    if (!(result instanceof Map)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    final Map<String, Number> map = (Map<String, Number>) result;
    return map.containsKey(CLUSTER_ID) && map.containsKey(CLUSTER_POSITION);
  }
}
