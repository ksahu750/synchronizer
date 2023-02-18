package com.ksahu750.synchronizer.offset;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class OffsetHolder {

  @Value(
      "${offset.file.path:/Users/kunalsahu/personal-projects/synchronizer/src/main/resources/offset}")
  private String filePath;

  private Path path;

  @PostConstruct
  public void init() {
    this.path = initialiseOffsetFile();
  }

  public long currentOffset() {
    try {
      return Long.parseLong(new String(Files.readAllBytes(path)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Path initialiseOffsetFile() {
    Path path = Paths.get(filePath);
    if (Files.exists(path)) {
      return path;
    }
    try {
      Files.writeString(path, "0", StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      return path;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void updateOffset(long offset) {
    try {
      Files.write(
          path,
          String.valueOf(offset).getBytes(),
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
