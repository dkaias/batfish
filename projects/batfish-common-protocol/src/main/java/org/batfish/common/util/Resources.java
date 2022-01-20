package org.batfish.common.util;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;

/** Utility class for reading resources in the classpath */
public final class Resources {

  /**
   * Returns the contents of the resource at the provided path as a string with the given charset.
   */
  public static @Nonnull String readResource(String resourcePath, Charset charset) {
    return new String(readResourceBytes(resourcePath), charset);
  }

  /** Returns the contents of the resource at the provided path as a byte array. */
  public static @Nonnull byte[] readResourceBytes(@Nonnull String resourcePath) {
    try (InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath)) {
      checkArgument(is != null, "Error opening resource: '%s'", resourcePath);
      return IOUtils.toByteArray(is);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not open resource: '" + resourcePath + "'", e);
    }
  }

  public static @Nonnull String readFile(String filePath, Charset charset) throws IOException {
    Path configFilePath = Paths.get(filePath);
    return new String(Files.readAllBytes(configFilePath), charset);
  }

  private Resources() {}
}
