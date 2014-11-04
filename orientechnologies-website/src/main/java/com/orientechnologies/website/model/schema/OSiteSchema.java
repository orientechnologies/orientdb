package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.*;

public class OSiteSchema {

  private static Map<Class<?>, EnumSet<?>> schemas  = new HashMap<Class<?>, EnumSet<?>>();

  private static List<Class<?>>            vertices = new ArrayList<Class<?>>();

  private static List<Class<?>>            edges    = new ArrayList<Class<?>>();
  static {
    addVertexClass(OComment.class);
    addVertexClass(OIssue.class);
    addVertexClass(Client.class);
    addVertexClass(ORepository.class);
    addVertexClass(OUser.class);
    addVertexClass(OOrganization.class);

    addEdgeClass(HasComment.class);
    addEdgeClass(HasDeveloper.class);
    addEdgeClass(HasIssue.class);
    addEdgeClass(HasMember.class);
    addEdgeClass(HasRepo.class);
    addEdgeClass(HasClient.class);
  }

  public static void addVertexClass(Class cls) {

    vertices.add(cls);
    addSchemaClass(cls);
  }

  public static void addSchemaClass(Class cls) {

    schemas.put(cls, EnumSet.allOf(cls));

  }

  public static void addEdgeClass(Class cls) {
    edges.add(cls);
    addSchemaClass(cls);
  }

  public static void createSchema(ODatabaseDocumentTx db) {

    if (!db.exists()) {
      db.create();
      fillSchema(db);
    }

  }

  public static void fillSchema(ODatabaseDocumentTx db) {
    OSchema schema = db.getMetadata().getSchema();

    OClass v = schema.getClass("V");
    OClass e = schema.getClass("E");

    for (Class<?> clazz : schemas.keySet()) {
      OClass cls = schema.createClass(clazz);
      if (vertices.contains(clazz)) {
        cls.setSuperClass(v);
      } else {
        cls.setSuperClass(e);
      }
      if (OTypeHolder.class.isAssignableFrom(clazz)) {
        for (Enum<?> anEnum : schemas.get(clazz)) {
          OType t = ((OTypeHolder) anEnum).getType();
          cls.createProperty(anEnum.toString(), t);
        }
      }
    }

    OSiteSchemaPopupator.populateData(db);
  }


    // Edges


}
