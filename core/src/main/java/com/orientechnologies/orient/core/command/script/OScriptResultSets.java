package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import java.util.Collections;

/**
 * Static Factories of OScriptResultSet objects
 *
 * <p>Used in script results with conversion to OResult for single iteration Created by Enrico Risa
 * on 27/01/17.
 */
public class OScriptResultSets {

  /**
   * Empty result set
   *
   * @return
   */
  public static OScriptResultSet empty() {
    return new OScriptResultSet(Collections.EMPTY_LIST.iterator(), null);
  }

  /**
   * Result set with a single result;
   *
   * @return
   */
  public static OScriptResultSet singleton(Object entity, OScriptTransformer transformer) {
    return new OScriptResultSet(Collections.singletonList(entity).iterator(), transformer);
  }
}
