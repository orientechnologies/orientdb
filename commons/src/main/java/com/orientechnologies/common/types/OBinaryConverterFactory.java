package com.orientechnologies.common.types;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
public class OBinaryConverterFactory {
  private static final OBinaryConverter INSTANCE;

  static {
    Class<?> sunClass = null;
    try {
      sunClass = Class.forName("sun.misc.Unsafe");
    } catch (Exception e) {
      // ignore
    }

    if (sunClass == null)
      INSTANCE = new OSafeBinaryConverter();
    else
      INSTANCE = new OUnsafeBinaryConverter();
  }

  public static OBinaryConverter getConverter() {
    return INSTANCE;
  }
}
