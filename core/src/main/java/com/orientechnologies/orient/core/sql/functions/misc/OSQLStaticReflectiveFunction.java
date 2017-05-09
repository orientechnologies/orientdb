package com.orientechnologies.orient.core.sql.functions.misc;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * This {@link OSQLFunction} is able to invoke a static method using reflection. If contains more than one {@link Method} it tries
 * to pick the one that better fits the input parameters.
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

  private static final Map<Class<?>, Integer>  PRIMITIVE_WEIGHT     = new HashMap<>();
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

  private Method[]                             methods;

  public OSQLStaticReflectiveFunction(String name, int minParams, int maxParams, Method... methods) {
    super(name, minParams, maxParams);
    this.methods = methods;
    // we need to sort the methods by parameters type to return the closest overloaded method
    Arrays.sort(methods, (m1, m2) -> {
      Class<?>[] m1Params = m1.getParameterTypes();
      Class<?>[] m2Params = m2.getParameterTypes();

      int c = m1Params.length - m2Params.length;
      if (c == 0) {
        for (int i = 0; i < m1Params.length; i++) {
          if (m1Params[i].isPrimitive() && m2Params[i].isPrimitive() && !m1Params[i].equals(m2Params[i])) {
            c += PRIMITIVE_WEIGHT.get(m1Params[i]) - PRIMITIVE_WEIGHT.get(m2Params[i]);
          }
        }
      }

      return c;
    });
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    Method method = pickMethod(iParams);

    try {
      return method.invoke(null, iParams);
    } catch (ReflectiveOperationException e) {
      throw new OQueryParsingException("Error on executing method " + method + " with parameters " + Arrays.toString(iParams));
    }

  }

  @Override
  public String getSyntax() {
    return this.getName();
  }

  private Method pickMethod(Object[] iParams) {
    Method method = null;

    boolean match = false;
    for (Method m : methods) {
      Class<?>[] parameterTypes = m.getParameterTypes();
      if (iParams.length == parameterTypes.length) {
        for (int i = 0; i < parameterTypes.length; i++) {
          if (isAssignable(iParams[i].getClass(), parameterTypes[i])) {
            match = true;
            break;
          }
        }

        if (iParams.length == 0 || match) {
          method = m;
          break;
        }
      }
    }

    return method;
  }

  private static boolean isAssignable(Class<?> cls, Class<?> toClass) {
    // handle autoboxing
    if (cls.isPrimitive() && !toClass.isPrimitive()) {
      cls = PRIMITIVE_TO_WRAPPER.get(cls);
      if (cls == null) {
        return false;
      }
    }
    if (toClass.isPrimitive() && !cls.isPrimitive()) {
      cls = WRAPPER_TO_PRIMITIVE.get(cls);
      if (cls == null) {
        return false;
      }
    }

    if (cls.equals(toClass)) {
      return true;
    }
    if (cls.isPrimitive()) {
      if (!toClass.isPrimitive()) {
        return false;
      }
      if (Integer.TYPE.equals(cls)) {
        return Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
      }
      if (Long.TYPE.equals(cls)) {
        return Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
      }
      if (Boolean.TYPE.equals(cls)) {
        return false;
      }
      if (Double.TYPE.equals(cls)) {
        return false;
      }
      if (Float.TYPE.equals(cls)) {
        return Double.TYPE.equals(toClass);
      }
      if (Character.TYPE.equals(cls)) {
        return Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass)
            || Double.TYPE.equals(toClass);
      }
      if (Short.TYPE.equals(cls)) {
        return Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass)
            || Double.TYPE.equals(toClass);
      }
      if (Byte.TYPE.equals(cls)) {
        return Short.TYPE.equals(toClass) || Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass)
            || Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
      }
      // this should never happen
      return false;
    }
    return toClass.isAssignableFrom(cls);
  }

}