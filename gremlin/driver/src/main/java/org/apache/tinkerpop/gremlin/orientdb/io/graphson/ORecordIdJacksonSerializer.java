package org.apache.tinkerpop.gremlin.orientdb.io.graphson;

import com.orientechnologies.orient.core.id.ORecordId;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;

/** Created by Enrico Risa on 06/09/2017. */
public final class ORecordIdJacksonSerializer extends JsonSerializer<ORecordId> {

  @Override
  public void serialize(ORecordId value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeStartObject();
    jgen.writeFieldName(OrientIoRegistry.CLUSTER_ID);
    jgen.writeNumber(value.getClusterId());
    jgen.writeFieldName(OrientIoRegistry.CLUSTER_POSITION);
    jgen.writeNumber(value.getClusterPosition());
    jgen.writeEndObject();
  }

  @Override
  public void serializeWithType(
      ORecordId value, JsonGenerator jgen, SerializerProvider serializers, TypeSerializer typeSer)
      throws IOException {
    typeSer.writeTypePrefixForScalar(value, jgen);
    jgen.writeString(value.toString());
    typeSer.writeTypeSuffixForScalar(value, jgen);
  }
}
