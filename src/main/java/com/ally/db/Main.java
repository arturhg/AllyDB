package com.ally.db;

import com.ally.db.proto.impl.AllyServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public final class Main {

    public static void main(String[] args) {

        Ally ally = new Ally(ConfigLoader.READ_CACHE_SIZE(), ConfigLoader.WRITE_BUFFER_SIZE(), ConfigLoader.EDIT_BUFFER_SIZE(), ConfigLoader.STORAGE_FILE_RECOMMENDED_SIZE());

        Server server = ServerBuilder
                .forPort(ConfigLoader.GRPC_PORT())
                .addService(ProtoReflectionService.newInstance())
                .addService(new AllyServiceImpl(ally)).build();

        try {
            server.start();
            server.awaitTermination();
        } catch (IOException e) {
            log.error("Failed to start server", e);
        } catch (InterruptedException e) {
            log.error("Server interrupted");
        }
    }
}
