package com.orientechnologies.common.serialization;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
public class OBinaryConverterFactory {
  private static final boolean unsafeWasDetected;

  static {
    boolean unsafeDetected = false;

    try {
      Class<?> sunClass = Class.forName("sun.misc.Unsafe");
      unsafeDetected = sunClass != null;
    } catch (ClassNotFoundException cnfe) {
      // Ignore
    }

    unsafeWasDetected = unsafeDetected;
  }

  public static OBinaryConverter getConverter() {
    boolean useUnsafe = Boolean.valueOf(System.getProperty("memory.useUnsafe"));

    if (useUnsafe && unsafeWasDetected)
      return OUnsafeBinaryConverter.INSTANCE;

    return OSafeBinaryConverter.INSTANCE;
  }
}
