package com.orientechnologies.nio;

import com.sun.jna.Platform;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 11/20/13
 */
public class OCLibraryFactory {
  public static final OCLibraryFactory INSTANCE = new OCLibraryFactory();

  private static final CLibrary        C_LIBRARY;

  static {
    if (Platform.isAIX())
      C_LIBRARY = new AIXCLibrary();
    else
      C_LIBRARY = new GeneralCLibrary();
  }

  public CLibrary library() {
    return C_LIBRARY;
  }
}
