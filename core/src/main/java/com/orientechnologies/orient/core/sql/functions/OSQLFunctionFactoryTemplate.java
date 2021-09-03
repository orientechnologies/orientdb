package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Created by frank on 25/05/2017. */
public abstract class OSQLFunctionFactoryTemplate implements OSQLFunctionFactory {

  private final Map<String, Object> functions;

  public OSQLFunctionFactoryTemplate() {
    functions = new HashMap<>();
  }

  protected void register(final OSQLFunction function) {
    functions.put(function.getName().toLowerCase(Locale.ENGLISH), function);
  }

  protected void register(String name, Object function) {
    functions.put(name.toLowerCase(Locale.ENGLISH), function);
  }

  @Override
  public boolean hasFunction(final String name) {
    return functions.containsKey(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return functions.keySet();
  }

  @Override
  public OSQLFunction createFunction(final String name) throws OCommandExecutionException {
    final Object obj = functions.get(name);

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

  public Map<String, Object> getFunctions() {
    return functions;
  }
}
