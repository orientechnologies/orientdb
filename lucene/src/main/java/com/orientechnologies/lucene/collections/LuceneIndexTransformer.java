package com.orientechnologies.lucene.collections;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;
import java.util.stream.Stream;

/** Created by frank on 03/05/2017. */
public final class LuceneIndexTransformer {
  public static Stream<ORawPair<Object, ORID>> transformToStream(
      OLuceneResultSet resultSet, Object key) {
    return resultSet.stream()
        .map((identifiable -> new ORawPair<>(key, identifiable.getIdentity())));
  }
}
