package com.orientechnologies.common.console;

import com.orientechnologies.common.concur.resource.OCloseable;

/**
 * @author <a href='mailto:enisher@gmail.com'> Artem Orobets </a>
 */
public interface OCommandStream extends OCloseable {
  boolean hasNext();

  String nextCommand();
}
