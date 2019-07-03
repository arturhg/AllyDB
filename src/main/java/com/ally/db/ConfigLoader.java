package com.ally.db;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Accessors(fluent = true)
public final class ConfigLoader {

    private static final String ALLY_PROPERTIES = "ally.properties";
    @Getter
    private static final int READ_CACHE_SIZE;
    @Getter
    private static final int WRITE_BUFFER_SIZE;
    @Getter
    private static final int EDIT_BUFFER_SIZE;
    @Getter
    private static final long STORAGE_FILE_RECOMMENDED_SIZE;
    @Getter
    private static final int GRPC_PORT;

    static {
        Properties properties = loadProperties();

        READ_CACHE_SIZE = Integer.parseInt(properties.getProperty("read_cache_size"));
        WRITE_BUFFER_SIZE = Integer.parseInt(properties.getProperty("write_buffer_size"));
        EDIT_BUFFER_SIZE = Integer.parseInt(properties.getProperty("edit_buffer_size"));
        STORAGE_FILE_RECOMMENDED_SIZE = Long.parseLong(properties.getProperty("storage_file_recommended_size"));
        GRPC_PORT = Integer.parseInt(properties.getProperty("grpc_port"));
    }

    private ConfigLoader() {

    }

    private static Properties loadProperties() {
        String resourceName = ALLY_PROPERTIES;
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        Properties properties = new Properties();

        try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            properties.load(resourceStream);
        } catch (IOException e) {
            log.error("Failed to load properties", e);
            System.exit(-1);
        }
        return properties;
    }
}
