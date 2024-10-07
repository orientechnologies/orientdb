package org.apache.tinkerpop.gremlin.orientdb.io.graphson;

import com.orientechnologies.orient.core.id.ORecordId;
import java.io.IOException;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;

/** Created by Enrico Risa on 06/09/2017. */
public class ORecordIdJacksonDeserializer extends StdDeserializer<ORecordId> {
  protected ORecordIdJacksonDeserializer() {
    super(ORecordId.class);
  }

  @Override
  public ORecordId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JsonProcessingException {
    String rid = deserializationContext.readValue(jsonParser, String.class);
    return new ORecordId(rid);
  }
}
