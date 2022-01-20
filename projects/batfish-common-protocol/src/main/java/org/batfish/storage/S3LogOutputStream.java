package org.batfish.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;

public class S3LogOutputStream extends ByteArrayOutputStream {
    private final String _logKey;
    private final S3BucketStorage _s3Storage;

    public S3LogOutputStream(String logKey, Path baseDir, String configFile) throws IOException {
       _logKey = logKey;
       _s3Storage = new S3BucketStorage(configFile, baseDir, null);
    }

    @Override
    public void flush() throws IOException {
        throw new IOException("not implemented");
    }

    @Override
    public void close() throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(toByteArray());
        _s3Storage.writeStreamToFile(inputStream, Path.of(_logKey));
    }
}
