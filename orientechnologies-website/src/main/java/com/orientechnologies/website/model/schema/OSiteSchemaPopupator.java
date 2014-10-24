package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Created by enricorisa on 16/10/14.
 */
public class OSiteSchemaPopupator {

  public static void populateData(ODatabaseDocumentTx db) {

    OrientGraph graph = new OrientGraph(db);

//    OrientVertex org = graph.addVertex("class:" + OSiteSchema.Organization.class.getSimpleName(), new Object[] {
//        OSiteSchema.Organization.NAME.toString(), "Orient Technologies", OSiteSchema.Organization.CODENAME.toString(),
//        "orientechnologies" });
//
//    Vertex member = graph.addVertex("class:" + OSiteSchema.Member.class.getSimpleName(),
//
//    new Object[] { OSiteSchema.Member.NAME.toString(), "Enrico Risa", OSiteSchema.Member.CODENAME.toString(), "maggiolo00", "id1",
//        9999990000028175918D });
//
//    OrientVertex repo = graph.addVertex("class:" + OSiteSchema.Repository.class.getSimpleName(), new Object[] {
//        OSiteSchema.Repository.NAME.toString(), "orientdb" });
//
//    org.addEdge(OSiteSchema.HasMember.class.getSimpleName(), member);
//    org.addEdge(OSiteSchema.HasRepo.class.getSimpleName(), repo);
//
//    graph.commit();
  }
}
