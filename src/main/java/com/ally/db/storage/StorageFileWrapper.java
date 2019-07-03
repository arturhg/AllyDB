package com.ally.db.storage;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

@Slf4j
public class StorageFileWrapper {

    private static final Pattern DASH = Pattern.compile("-");
    private static final String FILE_EXTENSION = ".abby";
    private static final char STORAGE_FILE_PREFIX = 's';
    private static final String UNDERSCORE = "_";

    private final File storageFile;

    @Getter
    private long numberOfLines = 1;

    public StorageFileWrapper(String dbDirectoryPath) throws IOException {

        String filename = STORAGE_FILE_PREFIX + DASH.matcher(UUID.randomUUID().toString()).replaceAll(UNDERSCORE) + FILE_EXTENSION;

        File file = new File(dbDirectoryPath + File.separator + filename);

        if (file.createNewFile()) {
            log.info("Storage file was created");
        } else {
            throw new IOException("Failed to create storage file");
        }

        storageFile = file;

    }

    public StorageFileWrapper(File file) throws IOException {

        if (file.exists()) {
            storageFile = file;
        } else {
            throw new IOException("File not found");
        }

        try {
            numberOfLines = Files.readAllLines(Paths.get(file.getPath())).size();
        } catch (IOException e) {
            throw new IOException("Failed to count lines of the file", e);
        }

    }

    public List<String> allLines() {
        try {
            return Files.readAllLines(Paths.get(storageFile.getPath()));
        } catch (IOException e) {
            log.error("Failed to load lines from storage file", e);
            System.exit(-1);
        }
        return new ArrayList<>();
    }

    public void truncate() {
        try {
            Files.write(Paths.get(storageFile.getPath()), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
            numberOfLines = 1;
        } catch (IOException e) {
            log.error("Failed to truncate the storage file", e);
            System.exit(-1);
        }
    }

    public long getSizeInBytes() {
        return storageFile.length();
    }

    public void appendLine(String line, String separator) throws IOException {
        try {

            if (separator == null) {
                separator = "";
            }

            Files.write(Paths.get(storageFile.getPath()), (line + separator).getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);

            numberOfLines++;
        } catch (IOException e) {
            throw new IOException("Failed to append a line to the file", e);
        }

    }

    public String getFileName() {
        return storageFile.getName();
    }


}
