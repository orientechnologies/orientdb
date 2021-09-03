package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OLuceneClassIndexContext {

  protected final OIndexDefinition definition;
  protected final String name;
  protected final boolean automatic;
  protected final ODocument metadata;
  protected final Map<String, Boolean> fieldsToStore = new HashMap<String, Boolean>();
  protected final OClass indexClass;

  public OLuceneClassIndexContext(
      OSchema schema,
      OIndexDefinition definition,
      String name,
      boolean automatic,
      ODocument metadata) {
    this.definition = definition;
    this.name = name;
    this.automatic = automatic;
    this.metadata = metadata;

    OLogManager.instance().info(this, "index definition:: " + definition);

    indexClass = schema.getClass(definition.getClassName());

    updateFieldToStore(definition);
  }

  private void updateFieldToStore(OIndexDefinition indexDefinition) {

    List<String> fields = indexDefinition.getFields();

    for (String field : fields) {
      OProperty property = indexClass.getProperty(field);

      if (property.getType().isEmbedded() && property.getLinkedType() != null) {
        fieldsToStore.put(field, true);
      } else {
        fieldsToStore.put(field, false);
      }
    }
  }

  public boolean isFieldToStore(String field) {
    if (fieldsToStore.containsKey(field)) return fieldsToStore.get(field);
    return false;
  }
}
