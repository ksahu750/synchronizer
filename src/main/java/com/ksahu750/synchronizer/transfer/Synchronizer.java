package com.ksahu750.synchronizer.transfer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Synchronizer {

  public void sync(String file, String source, String dest) {
    final Instant start = Instant.now();
    try (final FileInputStream is = new FileInputStream(source + "/" + file);
        final FileOutputStream os = new FileOutputStream(dest + "/" + file)) {
      final FileChannel srcChannel = is.getChannel();
      final FileChannel dstChannel = os.getChannel();

      srcChannel.transferTo(0, srcChannel.size(), dstChannel);

      log.info(
          "File {} of size {} synced in {} ms",
          file,
          srcChannel.size(),
          Instant.now().toEpochMilli() - start.toEpochMilli());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
