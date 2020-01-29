package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;

import java.util.stream.Stream;

public class IndexStreamSecurityDecorator {
  public static Stream<ORawPair<Object, ORID>> decorateStream(OIndex originalIndex, Stream<ORawPair<Object, ORID>> stream) {
    return stream.filter((pair) -> OIndexInternal.securityFilterOnRead(originalIndex, pair.second) != null);
  }

  public static Stream<ORID> decorateRidStream(OIndex originalIndex, Stream<ORID> stream) {
    return stream.filter((rid) -> OIndexInternal.securityFilterOnRead(originalIndex, rid) != null);
  }
}