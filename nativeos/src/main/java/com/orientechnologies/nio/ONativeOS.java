/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.nio;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * @author Andrey Lomakin
 * @since 22.04.13
 */
public class ONativeOS {
  private static final String LIBRARY_NAME = "nativeos";

  public static native long crc32(Pointer dataPtr, int size);

  static {
    final String resourceName = getResourceName();

    URL url = ONativeOS.class.getResource(resourceName);
    if (url == null)
      throw new IllegalStateException("Native library was not found in " + resourceName + " path.");

    try {
      File lib;

      if (url.getProtocol().toLowerCase().equals("file"))
        lib = new File(url.toURI());
      else {
        InputStream is = Native.class.getResourceAsStream(resourceName);
        FileOutputStream fos = null;

        try {
          lib = File.createTempFile(LIBRARY_NAME, Platform.isWindows() ? ".dll" : null);
          lib.deleteOnExit();
          fos = new FileOutputStream(lib);

          int count;
          byte[] buf = new byte[1024];
          while ((count = is.read(buf, 0, buf.length)) > 0) {
            fos.write(buf, 0, count);
          }
        } finally {
          try {
            is.close();
          } catch (IOException e) {
          }
          if (fos != null) {
            try {
              fos.close();
            } catch (IOException e) {
            }
          }
        }

      }

      Native.register(lib.getAbsolutePath());

    } catch (IOException e) {
      throw new IllegalStateException("Error during library load", e);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Error during library load", e);
    }
  }

  private static String getResourceName() {
    String prefix;
    if (Platform.isWindows())
      prefix = "win32-" + (Platform.is64Bit() ? "amd64" : "x86");
    else if (Platform.isLinux())
      prefix = "linux-" + (Platform.is64Bit() ? "amd64" : "i386");
    else
      return null;

    return "/com/orientechnologies/nio/" + prefix + "/" + System.mapLibraryName(LIBRARY_NAME);
  }
}
