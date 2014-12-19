package com.orientechnologies.website.helper;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientElement;

/**
 * Created by Enrico Risa on 18/12/14.
 */
public class OSequenceHelper {

  public static long next(OrientBaseGraph graph, String klass) {
    String query = String.format(
        "update (select from  OSequence  where className = '%s' ) increment value = 1 RETURN before @this", klass);
    Iterable<OrientElement> iterable = graph.command(new OCommandSQL(query)).execute();
    return iterable.iterator().next().getProperty("value");
  }
}
