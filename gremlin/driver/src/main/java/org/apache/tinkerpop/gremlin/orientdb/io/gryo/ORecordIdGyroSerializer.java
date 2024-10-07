package org.apache.tinkerpop.gremlin.orientdb.io.gryo;

import com.orientechnologies.orient.core.id.ORecordId;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

/** Created by Enrico Risa on 06/09/2017. */
public class ORecordIdGyroSerializer extends Serializer<ORecordId> {
  @Override
  public ORecordId read(
      final Kryo kryo, final Input input, final Class<ORecordId> tinkerGraphClass) {
    return new ORecordId(input.readString());
  }

  @Override
  public void write(final Kryo kryo, final Output output, final ORecordId rid) {
    output.writeString(rid.toString());
  }
}
