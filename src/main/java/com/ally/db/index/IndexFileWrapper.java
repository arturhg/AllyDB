package com.ally.db.index;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class IndexFileWrapper {
    private static final String INDEX_NAME = "index";
    private static final String DB_DIRECTORY = "./db";
    private static final String FILE_EXTENSION = ".abby";
    private final File index;

    public IndexFileWrapper() {
        index = new File(DB_DIRECTORY + File.separator + INDEX_NAME + FILE_EXTENSION);
    }

    public boolean exists() {
        return index.exists() || index.length() != 0;
    }

    public void createIndex() {
        try {
            boolean newIndexCreated = index.createNewFile();
            if (!newIndexCreated) {
                log.error("Index file already exists");
                System.exit(-1);
            }
        } catch (IOException e) {
            log.error("Cannot create index file", e);
            System.exit(-1);
        }
    }

    public void deleteIndex() {
        boolean indexWasDeleted = index.delete();
        if (!indexWasDeleted) {
            log.error("Cannot delete old index");
            System.exit(-1);
        }
    }

    public Map<String, ValuePointer> loadIndex() {

        Map<String, ValuePointer> inMemoryIndex = null;
        try {
            inMemoryIndex = Files.readAllLines(Paths.get(index.getPath()), StandardCharsets.UTF_8).stream()
                    .map(element -> element.split("\\|"))
                    .collect(Collectors.toMap(elementArray -> elementArray[0], elementArray -> new ValuePointer(elementArray[1], Long.parseLong(elementArray[2]))));
        } catch (IOException e) {
            log.error("Failed to load index", e);
            System.exit(-1);
        }

        return inMemoryIndex;

    }

    public void writeIndexOnDisk(Map<String, ValuePointer> inMemoryIndex) {
        Map<String, ValuePointer> indexFromDisk = loadIndex();

        if (indexFromDisk.entrySet().equals(inMemoryIndex.entrySet())) {
            return;
        }

        writeChangedIndex(inMemoryIndex);
    }

    private void writeChangedIndex(Map<String, ValuePointer> inMemoryIndex) {

        truncate();

        inMemoryIndex.forEach((key, value) -> {
            try {
                Files.write(Paths.get(index.getPath()),
                        (key + '|' + value.getFilename() + '|' + value.getLineNumber() + System.lineSeparator())
                                .getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            } catch (IOException e) {
                log.error("Failed to write index on disk", e);
                System.exit(-1);
            }
        });

        log.info("Index was written on disk");

    }

    private void truncate() {
        try {
            Files.write(Paths.get(index.getPath()), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.error("Failed to truncate the storage file", e);
            System.exit(-1);
        }
    }
}
