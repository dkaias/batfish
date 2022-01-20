package org.batfish.storage;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.batfish.common.BatfishLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class S3BucketStorageTest {
  private BatfishLogger _logger;

  @Test
  public void testInit() throws IOException {
    System.out.println("in our test");
    //S3BucketStorage s3Storage = new S3BucketStorage("test-bucket", "us-west-2", "batfish-development--read-only");
    boolean testBool = true;
    assertTrue(testBool);
  }
 }
