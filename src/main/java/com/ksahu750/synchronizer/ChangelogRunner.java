package com.ksahu750.synchronizer;

import com.ksahu750.synchronizer.changelog.ChangelogReader;
import com.ksahu750.synchronizer.changelog.ChangelogReader.ChangelogIterator;
import com.ksahu750.synchronizer.offset.OffsetHolder;
import com.ksahu750.synchronizer.transfer.Synchronizer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

@Service
public class ChangelogRunner implements CommandLineRunner {

  private final ChangelogReader changelogReader;
  private final OffsetHolder offsetHolder;
  private final Synchronizer synchronizer;

  public ChangelogRunner(
      ChangelogReader changelogReader, OffsetHolder offsetHolder, Synchronizer synchronizer) {
    this.changelogReader = changelogReader;
    this.offsetHolder = offsetHolder;
    this.synchronizer = synchronizer;
  }

  @Override
  public void run(String... args) throws Exception {
    final long offset = offsetHolder.currentOffset();
    try (final ChangelogIterator changelogIterator = changelogReader.fromOffset(offset)) {
      changelogIterator.stream()
          //          .limit(2)
          .forEach(
              file ->
                  synchronizer.sync(
                      file,
                      "/Users/kunalsahu/Desktop/testsrc",
                      "/Users/kunalsahu/Desktop/testdst"));

      offsetHolder.updateOffset(offset + changelogIterator.bytesRead());
    }
  }
}
