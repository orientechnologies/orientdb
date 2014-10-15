package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class OSiteSchema {

  public static void createSchema(ODatabaseDocumentTx db) {

    if (!db.exists()) {

      db.create();
      OSchema schema = db.getMetadata().getSchema();

      OClass v = schema.getClass("V");

      OClass todo = schema.createClass(Issue.class);
      todo.setSuperClass(v);
      todo.createProperty(Issue.DESCRIPTION.toString(), OType.STRING);
      todo.createProperty(Issue.CREATED_AT.toString(), OType.DATETIME);

    }

  }

  public enum Issue {
    DESCRIPTION("description"), CREATED_AT("createdAt");
    private final String description;

    Issue(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

}
