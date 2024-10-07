package org.apache.tinkerpop.gremlin.orientdb.io.gryo;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

/** Created by Enrico Risa on 06/09/2017. */
public class ORidBagGyroSerializer extends Serializer<ORidBag> {
  @Override
  public ORidBag read(final Kryo kryo, final Input input, final Class<ORidBag> tinkerGraphClass) {
    final ORidBag bag = new ORidBag();
    final String[] ids = input.readString().split(";");

    for (final String id : ids) {
      bag.add(new ORecordId(id));
    }
    return bag;
  }

  @Override
  public void write(final Kryo kryo, final Output output, final ORidBag bag) {
    final StringBuilder ids = new StringBuilder();
    bag.forEach(rid -> ids.append(rid.getIdentity()).append(";"));
    output.writeString(ids);
  }
}
