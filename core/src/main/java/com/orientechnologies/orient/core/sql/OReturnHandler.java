package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public interface OReturnHandler {
  void reset();

  void beforeUpdate(ODocument result);

  void afterUpdate(ODocument result);

  /**
   * 
   * @return collected result
   */
  Object ret();
}
