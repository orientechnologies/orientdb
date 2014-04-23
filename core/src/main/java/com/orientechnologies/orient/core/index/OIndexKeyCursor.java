package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Map;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 4/22/14
 */
public interface OIndexKeyCursor {
  Object next(int prefetchSize);
}
