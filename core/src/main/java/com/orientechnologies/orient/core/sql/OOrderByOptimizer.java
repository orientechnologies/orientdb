package com.orientechnologies.orient.core.sql;

import java.util.List;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OOrderByOptimizer {
  boolean canBeUsedByOrderBy(OIndex<?> index, List<OPair<String, String>> orderedFields) {
    if (orderedFields.isEmpty())
      return false;

    if (!index.supportsOrderedIterations())
      return false;

    final OIndexDefinition definition = index.getDefinition();
    final List<String> fields = definition.getFields();
    final int endIndex = Math.min(fields.size(), orderedFields.size());

    final String firstOrder = orderedFields.get(0).getValue();
    for (int i = 0; i < endIndex; i++) {
      final OPair<String, String> pair = orderedFields.get(i);

      if (!firstOrder.equals(pair.getValue()))
        return false;

      final String orderFieldName = orderedFields.get(i).getKey().toLowerCase();
      final String indexFieldName = fields.get(i).toLowerCase();

      if (!orderFieldName.equals(indexFieldName))
        return false;
    }

    return true;
  }
}
