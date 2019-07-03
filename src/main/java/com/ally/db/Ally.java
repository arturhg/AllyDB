package com.ally.db;

import com.ally.db.index.ValuePointer;
import com.ally.db.storage.StorageFileWrapper;
import com.ally.db.util.CompressionUtil;
import com.ally.db.util.HashUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;


@Slf4j
public final class Ally {

    private static final String INDEX_NAME = "index";
    private static final String TEMP_INDEX_ABBY = "tempIndex";
    private static final String DB_DIRECTORY = "./db";
    private static final String FILE_EXTENSION = ".abby";

    private final Object lock = new Object();
    private final Map<String, ValuePointer> inMemoryIndex = new ConcurrentHashMap<>();
    private final Set<String> dirtyFilesNames = new HashSet<>();
    private final Set<StorageFileWrapper> setOfStorageFileWrappers = new HashSet<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(4);

    private Map<String, byte[]> writeBuffer;
    private Map<String, byte[]> editBuffer;
    private Cache<String, byte[]> readCache;

    private long storageFileRecommendedSize;

    private StorageFileWrapper currentStorageFileWrapper;
    private File directory;
    private File index;

    private Ally() {

    }

    public Ally(int readCacheSize, int writeBufferSize, int editBufferSize, long storageFileRecommendedSize) {

        synchronized (lock) {

            writeBuffer = new HashMap<>(writeBufferSize);

            editBuffer = new HashMap<>(editBufferSize);

            readCache = Caffeine.newBuilder()
                    .maximumSize(readCacheSize)
                    .initialCapacity(10)
                    .build();

            this.storageFileRecommendedSize = storageFileRecommendedSize;

            //if dir does not exist, create it

            //if dir exists, but there are no files, create an index file and 1 storage file

            //if there are storage files, but no index file, remove all file and create index file and 1 storage file

            //if there is an index file and at least 1 storage file, choose the smallest storage file as an current file to write

            directory = createDbDirectory();

            index = new File(DB_DIRECTORY + File.separator + INDEX_NAME + FILE_EXTENSION);

            loadIndexAndStorage(directory, index);

            scheduledExecutorService.scheduleAtFixedRate(this::dumpWriteBufferToDisk, 5, 5, TimeUnit.SECONDS);
            scheduledExecutorService.scheduleAtFixedRate(this::dumpIndexToDisk, 5, 15, TimeUnit.SECONDS);
            scheduledExecutorService.scheduleAtFixedRate(this::dumpEditBufferToDisk, 10, 10, TimeUnit.SECONDS);
            scheduledExecutorService.scheduleAtFixedRate(this::gc, 30, 30, TimeUnit.SECONDS);
        }

    }

    public void put(byte[] key, byte[] value) {

        if (key == null || key.length == 0) {
            log.warn("Key must be not null or empty");
        }

        if (value == null || value.length == 0) {
            log.warn("Value must be not null or empty");
            return;
        }

        //hash the key
        String hashedKey = HashUtil.getSHA256Hash(key);

        //compress the value
        byte[] compressedValue = null;

        try {
            compressedValue = CompressionUtil.compress(value);
        } catch (IOException e) {
            log.error("Failed to compress data", e);
            System.exit(-1);
        }

        putElement(hashedKey, compressedValue);

    }

    public byte[] get(byte[] key) {

        if (key == null || key.length == 0) {
            log.error("Key must be not null or empty");
        }

        //hash the key
        String hashedKey = HashUtil.getSHA256Hash(key);

        byte[] decompressedValue = new byte[0];

        try {

            byte[] compressedValue = getElement(hashedKey);

            if (compressedValue != null && compressedValue.length != 0) {
                decompressedValue = CompressionUtil.decompress(compressedValue);
            }

        } catch (DataFormatException | IOException e) {
            log.error("Failed to decompress data", e);
            System.exit(-1);
        }

        return decompressedValue;


    }

    private void dumpWriteBufferToDisk() {
        synchronized (lock) {

            if (!writeBuffer.isEmpty()) {

                currentStorageFileWrapper = createNewStorageFileIfNeeded(currentStorageFileWrapper);

                //write buffer elements to current storage file
                //add hashes to in memory index
                writeBuffer.forEach((key, value) -> {
                    try {

                        currentStorageFileWrapper = createNewStorageFileIfNeeded(currentStorageFileWrapper);

                        long initialLine = currentStorageFileWrapper.getNumberOfLines();

                        currentStorageFileWrapper.appendLine(key + '|' + DatatypeConverter.printHexBinary(value), System.lineSeparator());

                        inMemoryIndex.put(key, new ValuePointer(currentStorageFileWrapper.getFileName(), initialLine));


                    } catch (IOException e) {
                        log.error("Failed to write into storage file from write buffer", e);
                        System.exit(-1);
                    }
                });

                writeBuffer.clear();
            }

        }
    }

    private void dumpEditBufferToDisk() {
        synchronized (lock) {

            if (!editBuffer.isEmpty()) {

                currentStorageFileWrapper = createNewStorageFileIfNeeded(currentStorageFileWrapper);

                editBuffer.forEach((key, value) -> {

                    try {

                        currentStorageFileWrapper = createNewStorageFileIfNeeded(currentStorageFileWrapper);

                        long initialLine = currentStorageFileWrapper.getNumberOfLines();

                        currentStorageFileWrapper.appendLine(key + '|' + DatatypeConverter.printHexBinary(value), System.lineSeparator());

                        inMemoryIndex.put(key, new ValuePointer(currentStorageFileWrapper.getFileName(), initialLine));

                        dirtyFilesNames.add(inMemoryIndex.get(key).getFilename());

                        inMemoryIndex.put(key, new ValuePointer(currentStorageFileWrapper.getFileName(), initialLine));

                    } catch (IOException e) {
                        log.error("Failed to write into storage file from edit buffer", e);
                        System.exit(-1);
                    }
                });

                editBuffer.clear();

            }

        }
    }

    private void dumpIndexToDisk() {
        synchronized (lock) {

            File tempIndex = new File(DB_DIRECTORY + File.separator + TEMP_INDEX_ABBY + FILE_EXTENSION);

            if (tempIndex.exists()) {
                boolean tempIndexDeleted = tempIndex.delete();
                if (tempIndexDeleted) {
                    log.info("Old temp index was deleted");
                } else {
                    log.error("Failed to delete old temp index");
                    System.exit(-1);
                }
            }

            try {
                boolean tempIndexCreated = tempIndex.createNewFile();
                if (!tempIndexCreated) {
                    log.error("Old temp index file already exists");
                    System.exit(-1);
                }
            } catch (IOException e) {
                log.error("Cannot create temp index file", e);
                System.exit(-1);
            }

            inMemoryIndex.forEach((key, value) -> {
                try {
                    Files.write(Paths.get(tempIndex.getPath()),
                            (key + '|' + value.getFilename() + '|' + value.getLineNumber() + System.lineSeparator())
                                    .getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                } catch (IOException e) {
                    log.error("Failed to write index on disk", e);
                    System.exit(-1);
                }
            });

            boolean indexRenamed = tempIndex.renameTo(index);
            if (indexRenamed) {
                log.info("Renamed index file");
            } else {
                log.error("Failed to rename index file");
                System.exit(-1);
            }

            log.info("Index was written on disk");

        }
    }

    private void gc() {
        synchronized (lock) {
            dirtyFilesNames.stream()
                    .map(dirtyFile -> DB_DIRECTORY + File.separator + dirtyFile)
                    .forEach(this::cleanDirtyFile);

            dirtyFilesNames.clear();

        }
    }

    private StorageFileWrapper createNewStorageFileIfNeeded(StorageFileWrapper storageFileWrapper) {

        if (storageFileWrapper.getSizeInBytes() > storageFileRecommendedSize) {

            setOfStorageFileWrappers.add(storageFileWrapper);

            try {
                return new StorageFileWrapper(DB_DIRECTORY);
            } catch (IOException e) {
                log.error("Failed to create new storage file", e);
            }
        }

        return storageFileWrapper;
    }

    private void cleanDirtyFile(String dirtyFileName) {

        try {

            StorageFileWrapper dirtyStorageFileWrapper = new StorageFileWrapper(new File(dirtyFileName));

            List<String> lines = dirtyStorageFileWrapper.allLines();

            log.info("Read content of dirty storage file to memory");

            dirtyStorageFileWrapper.truncate();

            log.info("Truncated the dirty storage file");

            lines.stream().filter(line -> {
                String key = line.split("\\|")[0];
                return inMemoryIndex.containsKey(key)
                        && inMemoryIndex.get(key).getFilename().equals(dirtyStorageFileWrapper.getFileName());
            })
                    .forEach(line -> {
                        try {

                            dirtyStorageFileWrapper.appendLine(line, "");

                            String key = line.split("\\|")[0];
                            ValuePointer oldValuePointer = inMemoryIndex.get(key);

                            inMemoryIndex.put(key, new ValuePointer(oldValuePointer.getFilename(), dirtyStorageFileWrapper.getNumberOfLines()));

                        } catch (IOException e) {
                            log.error("Failed to write into temp file from dirty file", e);
                            System.exit(-1);
                        }
                    });

            log.info("Wrote clean content to storage file");

        } catch (IOException e) {
            log.error("Failed to read lines from a file", e);
            System.exit(-1);
        }
    }

    private String readLineFromFile(File file, long lineNumber) {
        try (Stream<String> lines = Files.lines(Paths.get(file.getPath()))) {
            Optional<String> optionalLine = lines.skip(lineNumber - 1).findFirst();
            if (optionalLine.isPresent()) {
                return optionalLine.get();
            }
        } catch (IOException e) {
            log.error("Failed to read a line from file");
            System.exit(-1);
        }
        return null;
    }

    private void putElement(String hash, byte[] value) {

        synchronized (lock) {
            if (inMemoryIndex.containsKey(hash)) {
                editBuffer.put(hash, value);
            } else {
                writeBuffer.put(hash, value);
            }
        }


    }

    private byte[] getElement(String hash) {


        synchronized (lock) {
            if (readCache.asMap().containsKey(hash)) {
                return readCache.getIfPresent(hash);
            }
            if (writeBuffer.containsKey(hash)) {
                byte[] compressedValue = writeBuffer.get(hash);
                readCache.put(hash, compressedValue);
                return compressedValue;
            }
            if (editBuffer.containsKey(hash)) {
                byte[] compressedValue = editBuffer.get(hash);
                readCache.put(hash, compressedValue);
                return compressedValue;
            }

            if (inMemoryIndex.containsKey(hash)) {
                ValuePointer valuePointer = inMemoryIndex.get(hash);
                String filename = valuePointer.getFilename();
                File file = new File(DB_DIRECTORY + File.separator + filename);
                String line = readLineFromFile(file, valuePointer.getLineNumber());

                if (line != null && !line.isEmpty()) {
                    String hexedData = line.split("\\|")[1];

                    byte[] compressedValue = DatatypeConverter.parseHexBinary(hexedData);

                    readCache.put(hash, compressedValue);
                    return compressedValue;
                }


            }

            log.info("No value found for hashed key: {}", hash);
            return new byte[0];
        }

    }

    private void loadIndexAndStorage(File directory, File index) {

        File[] allRawStorageFiles = directory.listFiles((dir, name) -> name.startsWith("s") && name.endsWith(FILE_EXTENSION));

        if (index.exists() && !index.isDirectory() && index.length() != 0 && allRawStorageFiles != null && allRawStorageFiles.length != 0) {

            List<StorageFileWrapper> storageFileWrappers = Arrays.stream(allRawStorageFiles)
                    .filter(File::exists)
                    .map(rawStorageFile -> {
                        try {
                            return new StorageFileWrapper(rawStorageFile);
                        } catch (IOException e) {
                            log.error("Failed to create StorageFileWrapper", e);
                        }

                        return null;
                    }).filter(Objects::nonNull)
                    .collect(Collectors.toList());

            setOfStorageFileWrappers.addAll(storageFileWrappers);

            //read index into memory
            try {
                loadIndex(DB_DIRECTORY + File.separator + INDEX_NAME + FILE_EXTENSION);
            } catch (IOException e) {
                log.error("Failed to load index", e);
                System.exit(-1);
            }

            //choose smallest storage file as a current storage file
            StorageFileWrapper smallestStorageFileWrapper = storageFileWrappers.stream().min(Comparator.comparing(
                    StorageFileWrapper::getNumberOfLines
            )).orElseGet(null);

            currentStorageFileWrapper = createNewStorageFileIfNeeded(smallestStorageFileWrapper);

        } else {

            if (index.exists() && !index.isDirectory()) {

                //if there is no storage file, delete the index
                if (allRawStorageFiles == null || allRawStorageFiles.length == 0 || currentStorageFileWrapper == null) {
                    deleteIndex(index);
                }

            } else {

                //remove files in db directory
                cleanRawStorageFiles(allRawStorageFiles);
            }

            //create index
            createIndex(index);

            //create 1 storage file
            try {
                currentStorageFileWrapper = new StorageFileWrapper(DB_DIRECTORY);
            } catch (IOException e) {
                log.error("Failed to create new StorageFileWrapper");
                System.exit(-1);
            }
        }
    }

    private void loadIndex(String indexFileName) throws IOException {

        Files.readAllLines(Paths.get(indexFileName), StandardCharsets.UTF_8).stream()
                .map(element -> element.split("\\|"))
                .forEach(elementArray -> inMemoryIndex.put(elementArray[0],
                        new ValuePointer(elementArray[1], Long.parseLong(elementArray[2]))));

    }

    private void createIndex(File index) {
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

    private void deleteIndex(File index) {
        boolean indexWasDeleted = index.delete();
        if (!indexWasDeleted) {
            log.error("Cannot delete old index");
            System.exit(-1);
        }
    }

    private void cleanRawStorageFiles(File[] files) {
        if (files != null && files.length != 0) {
            for (File file : files) {
                boolean fileWasDeleted = file.delete();
                if (fileWasDeleted) {
                    log.info("Storage file was deleted");
                } else {
                    log.error("Failed to delete storage file");
                }
            }
        }
    }

    private File createDbDirectory() {
        File dbDirectory = new File(DB_DIRECTORY);
        if (!dbDirectory.exists()) {
            dbDirectory.mkdir();

        }
        return dbDirectory;
    }

}

