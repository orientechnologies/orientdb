package com.orientechnologies.orient.core.serialization.serializer.record;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
public class OSerializationLongIdThreadLocal extends ThreadLocal<Set<Long>> {
  public static OSerializationLongIdThreadLocal	INSTANCE	= new OSerializationLongIdThreadLocal();

  @Override
  protected Set<Long> initialValue() {
    return new HashSet<Long>();
  }
}
