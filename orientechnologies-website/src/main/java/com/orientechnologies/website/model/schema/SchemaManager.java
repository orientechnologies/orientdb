package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.reflections.Reflections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.persistence.Entity;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by Enrico Risa on 03/06/15.
 */

@Component
public class SchemaManager {

  @Autowired
  private OrientDBFactory dbFactory;

  public <T extends Identity> T fromDoc(ODocument document, Class<T> klass) {

    try {
      Identity entity = klass.newInstance();
      for (Map.Entry<String, Object> stringObjectEntry : document) {
        String fieldName = stringObjectEntry.getKey();
        Field field = getField(klass, fieldName);

        if (!fieldName.equals("id")) {
          if (field != null) {
            field.setAccessible(true);
            field.set(entity, stringObjectEntry.getValue());
          }
        }
      }
      entity.setId(document.getIdentity().toString());
      return (T) entity;
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return null;
  }

  protected Field getField(Class klass, String fieldName) {

    Field f = null;
    Class parentClass = klass;
    do {
      try {
        f = parentClass.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        parentClass = parentClass.getSuperclass();
      }
    } while (f == null && parentClass != null);

    return f;
  }

  protected Field[] getDeclaredFields(Class klass) {
    List<Field> fields = new ArrayList<Field>();

    Class parentClass = klass;
    do {
      fields.addAll(Arrays.asList(parentClass.getDeclaredFields()));
      parentClass = parentClass.getSuperclass();
    } while (parentClass != null);

    return fields.toArray(new Field[fields.size()]);
  }

  public ODocument toDoc(Identity entity) {

    ODocument doc;
    OrientGraph graph = dbFactory.getGraph();
    if (entity.getId() == null) {
      doc = new ODocument(entity.getClass().getSimpleName());
      entity.setUuid(java.util.UUID.randomUUID().toString());
    } else {
      doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
    }
    Field[] fields = getDeclaredFields(entity.getClass());

    for (Field field : fields) {
      try {
        field.setAccessible(true);
        doc.field(field.getName(), field.get(entity));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    return doc;
  }

  public void connect(Identity entity1, Identity entity2, String label) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = graph.getVertex(new ORecordId(entity1.getId()));
    OrientVertex devVertex = graph.getVertex(new ORecordId(entity2.getId()));
    orgVertex.addEdge(label, devVertex);
  }

  public void createSchema(String packageToScan) {
    Reflections reflections = new Reflections(packageToScan);
    Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(Entity.class);
    OrientBaseGraph graph = dbFactory.getGraphtNoTx();
    for (Class<?> aClass : typesAnnotatedWith) {
      graph.createVertexType(aClass.getSimpleName());
    }

  }
}
