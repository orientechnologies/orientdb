package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLStaticReflectiveFunction;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Factory for custom SQL functions.
 *
 * @author Fabrizio Fortino
 */
public class OCustomSQLFunctionFactory implements OSQLFunctionFactory {
  private static final Map<String, Object> FUNCTIONS = new HashMap<>();

  static {
    register("math_", Math.class);
  }

  public static void register(final String prefix, final Class<?> clazz) {
    final Map<String, List<Method>> methodsMap =
        Arrays.stream(clazz.getMethods())
            .filter(m -> Modifier.isStatic(m.getModifiers()))
            .collect(Collectors.groupingBy(Method::getName));

    for (Map.Entry<String, List<Method>> entry : methodsMap.entrySet()) {
      final String name = prefix + entry.getKey();
      if (FUNCTIONS.containsKey(name)) {
        OLogManager.instance()
            .warn(null, "Unable to register reflective function with name " + name);
      } else {
        List<Method> methodsList = methodsMap.get(entry.getKey());
        Method[] methods = new Method[methodsList.size()];
        int i = 0;
        int minParams = 0;
        int maxParams = 0;
        for (Method m : methodsList) {
          methods[i++] = m;
          minParams =
              minParams < m.getParameterTypes().length ? minParams : m.getParameterTypes().length;
          maxParams =
              maxParams > m.getParameterTypes().length ? maxParams : m.getParameterTypes().length;
        }
        FUNCTIONS.put(
            name.toLowerCase(Locale.ENGLISH),
            new OSQLStaticReflectiveFunction(name, minParams, maxParams, methods));
      }
    }
  }

  @Override
  public Set<String> getFunctionNames() {
    return FUNCTIONS.keySet();
  }

  @Override
  public boolean hasFunction(final String name) {
    return FUNCTIONS.containsKey(name);
  }

  @Override
  public OSQLFunction createFunction(final String name) {
    final Object obj = FUNCTIONS.get(name);

    if (obj == null) throw new OCommandExecutionException("Unknown function name :" + name);

    if (obj instanceof OSQLFunction) return (OSQLFunction) obj;
    else {
      // it's a class
      final Class<?> clazz = (Class<?>) obj;
      try {
        return (OSQLFunction) clazz.newInstance();
      } catch (Exception e) {
        throw OException.wrapException(
            new OCommandExecutionException(
                "Error in creation of function "
                    + name
                    + "(). Probably there is not an empty constructor or the constructor generates errors"),
            e);
      }
    }
  }
}
