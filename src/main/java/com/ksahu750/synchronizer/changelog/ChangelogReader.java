package com.ksahu750.synchronizer.changelog;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ChangelogReader {

  @Value(
      "${changelog.file.path:/Users/kunalsahu/personal-projects/synchronizer/src/main/resources/changelog}")
  private String filePath;

  public ChangelogIterator fromOffset(long offset) {
    return new ChangelogIterator(offset);
  }

  public class ChangelogIterator implements Iterator<String>, Closeable {

    private final BufferedReader reader;
    private final FileChannel channel;
    private final FileInputStream fis;

    private long bytesRead = 0;

    ChangelogIterator(long offset) {
      try {
        fis = new FileInputStream(filePath);
        channel = fis.getChannel().position(offset);
        this.reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private String nextLine = null;

    @Override
    public boolean hasNext() {
      if (nextLine != null) {
        return true;
      } else {
        try {
          nextLine = reader.readLine();
          return (nextLine != null);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    @Override
    public String next() {
      if (nextLine != null || hasNext()) {
        String line = nextLine;
        nextLine = null;
        // Adding 1 for newline (\n) char
        bytesRead += line.getBytes(StandardCharsets.UTF_8).length + 1;
        return line;
      } else {
        throw new NoSuchElementException();
      }
    }

    public long bytesRead() {
      return bytesRead;
    }

    public Stream<String> stream() {
      return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED | Spliterator.NONNULL),
          false);
    }

    @Override
    public void close() throws IOException {
      fis.close();
      channel.close();
      reader.close();
    }
  }
}
