package com.ally.db.proto.impl;

import com.ally.db.Ally;
import com.ally.proto.GetRequest;
import com.ally.proto.GetResponse;
import com.ally.proto.PutRequest;
import com.ally.proto.PutResponse;
import com.ally.proto.AllyServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

public class AllyServiceImpl extends AllyServiceGrpc.AllyServiceImplBase {

    private final Ally ally;

    public AllyServiceImpl(Ally ally) {
        this.ally = ally;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {

        byte[] key = request.getKey().toByteArray();

        byte[] value = ally.get(key);

        String responeStatus = value.length != 0 ? "Found" : "Not found";

        GetResponse getResponse = GetResponse.newBuilder()
                .setValue(ByteString.copyFrom(value))
                .setResponseStatus(responeStatus)
                .build();

        responseObserver.onNext(getResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {

        byte[] key = request.getKey().toByteArray();
        byte[] value = request.getValue().toByteArray();

        ally.put(key, value);

        PutResponse putResponse = PutResponse.newBuilder()
                .setResponseStatus("OK")
                .build();

        responseObserver.onNext(putResponse);
        responseObserver.onCompleted();
    }
}