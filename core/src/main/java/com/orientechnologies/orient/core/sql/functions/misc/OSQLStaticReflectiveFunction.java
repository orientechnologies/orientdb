package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This {@link OSQLFunction} is able to invoke a static method using reflection. If contains more
 * than one {@link Method} it tries to pick the one that better fits the input parameters.
 *
 * @author Fabrizio Fortino
 */
public class OSQLStaticReflectiveFunction extends OSQLFunctionAbstract {

  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER = new HashMap<>();

  static {
    PRIMITIVE_TO_WRAPPER.put(Boolean.TYPE, Boolean.class);
    PRIMITIVE_TO_WRAPPER.put(Byte.TYPE, Byte.class);
    PRIMITIVE_TO_WRAPPER.put(Character.TYPE, Character.class);
    PRIMITIVE_TO_WRAPPER.put(Short.TYPE, Short.class);
    PRIMITIVE_TO_WRAPPER.put(Integer.TYPE, Integer.class);
    PRIMITIVE_TO_WRAPPER.put(Long.TYPE, Long.class);
    PRIMITIVE_TO_WRAPPER.put(Double.TYPE, Double.class);
    PRIMITIVE_TO_WRAPPER.put(Float.TYPE, Float.class);
    PRIMITIVE_TO_WRAPPER.put(Void.TYPE, Void.TYPE);
  }

  private static final Map<Class<?>, Class<?>> WRAPPER_TO_PRIMITIVE = new HashMap<>();

  static {
    for (Class<?> primitive : PRIMITIVE_TO_WRAPPER.keySet()) {
      Class<?> wrapper = PRIMITIVE_TO_WRAPPER.get(primitive);
      if (!primitive.equals(wrapper)) {
        WRAPPER_TO_PRIMITIVE.put(wrapper, primitive);
      }
    }
  }

  private static final Map<Class<?>, Integer> PRIMITIVE_WEIGHT = new HashMap<>();

  static {
    PRIMITIVE_WEIGHT.put(boolean.class, 1);
    PRIMITIVE_WEIGHT.put(char.class, 2);
    PRIMITIVE_WEIGHT.put(byte.class, 3);
    PRIMITIVE_WEIGHT.put(short.class, 4);
    PRIMITIVE_WEIGHT.put(int.class, 5);
    PRIMITIVE_WEIGHT.put(long.class, 6);
    PRIMITIVE_WEIGHT.put(float.class, 7);
    PRIMITIVE_WEIGHT.put(double.class, 8);
    PRIMITIVE_WEIGHT.put(void.class, 9);
  }

  private Method[] methods;

  public OSQLStaticReflectiveFunction(
      String name, int minParams, int maxParams, Method... methods) {
    super(name, minParams, maxParams);
    this.methods = methods;
    // we need to sort the methods by parameters type to return the closest overloaded method
    Arrays.sort(
        methods,
        (m1, m2) -> {
          Class<?>[] m1Params = m1.getParameterTypes();
          Class<?>[] m2Params = m2.getParameterTypes();

          int c = m1Params.length - m2Params.length;
          if (c == 0) {
            for (int i = 0; i < m1Params.length; i++) {
              if (m1Params[i].isPrimitive()
                  && m2Params[i].isPrimitive()
                  && !m1Params[i].equals(m2Params[i])) {
                c += PRIMITIVE_WEIGHT.get(m1Params[i]) - PRIMITIVE_WEIGHT.get(m2Params[i]);
              }
            }
          }

          return c;
        });
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {

    final Supplier<String> paramsPrettyPrint =
        () ->
            Arrays.stream(iParams)
                .map(p -> p + " [ " + p.getClass().getName() + " ]")
                .collect(Collectors.joining(", ", "(", ")"));

    Method method = pickMethod(iParams);

    if (method == null) {
      throw new OQueryParsingException(
          "Unable to find a function for " + name + paramsPrettyPrint.get());
    }

    try {
      return method.invoke(null, iParams);
    } catch (ReflectiveOperationException e) {
      throw OException.wrapException(
          new OQueryParsingException("Error executing function " + name + paramsPrettyPrint.get()),
          e);
    } catch (IllegalArgumentException x) {
      OLogManager.instance().error(this, "Error executing function %s", x, name);

      return null; // if a function fails for given input, just return null to avoid breaking the
      // query execution
    }
  }

  @Override
  public String getSyntax() {
    return this.getName();
  }

  private Method pickMethod(Object[] iParams) {
    for (Method m : methods) {
      Class<?>[] parameterTypes = m.getParameterTypes();
      if (iParams.length == parameterTypes.length) {
        boolean match = true;
        for (int i = 0; i < parameterTypes.length; i++) {
          if (iParams[i] != null && !isAssignable(iParams[i].getClass(), parameterTypes[i])) {
            match = false;
            break;
          }
        }

        if (match) {
          return m;
        }
      }
    }

    return null;
  }

  private static boolean isAssignable(final Class<?> iFromClass, final Class<?> iToClass) {
    // handle autoboxing
    final BiFunction<Class<?>, Class<?>, Class<?>> autoboxer =
        (from, to) -> {
          if (from.isPrimitive() && !to.isPrimitive()) {
            return PRIMITIVE_TO_WRAPPER.get(from);
          } else if (to.isPrimitive() && !from.isPrimitive()) {
            return WRAPPER_TO_PRIMITIVE.get(from);
          } else return from;
        };

    final Class<?> fromClass = autoboxer.apply(iFromClass, iToClass);

    if (fromClass == null) {
      return false;
    } else if (fromClass.equals(iToClass)) {
      return true;
    } else if (fromClass.isPrimitive()) {
      if (!iToClass.isPrimitive()) {
        return false;
      } else if (Integer.TYPE.equals(fromClass)) {
        return Long.TYPE.equals(iToClass)
            || Float.TYPE.equals(iToClass)
            || Double.TYPE.equals(iToClass);
      } else if (Long.TYPE.equals(fromClass)) {
        return Float.TYPE.equals(iToClass) || Double.TYPE.equals(iToClass);
      } else if (Boolean.TYPE.equals(fromClass)) {
        return false;
      } else if (Double.TYPE.equals(fromClass)) {
        return false;
      } else if (Float.TYPE.equals(fromClass)) {
        return Double.TYPE.equals(iToClass);
      } else if (Character.TYPE.equals(fromClass)) {
        return Integer.TYPE.equals(iToClass)
            || Long.TYPE.equals(iToClass)
            || Float.TYPE.equals(iToClass)
            || Double.TYPE.equals(iToClass);
      } else if (Short.TYPE.equals(fromClass)) {
        return Integer.TYPE.equals(iToClass)
            || Long.TYPE.equals(iToClass)
            || Float.TYPE.equals(iToClass)
            || Double.TYPE.equals(iToClass);
      } else if (Byte.TYPE.equals(fromClass)) {
        return Short.TYPE.equals(iToClass)
            || Integer.TYPE.equals(iToClass)
            || Long.TYPE.equals(iToClass)
            || Float.TYPE.equals(iToClass)
            || Double.TYPE.equals(iToClass);
      }
      // this should never happen
      return false;
    }
    return iToClass.isAssignableFrom(fromClass);
  }
}
