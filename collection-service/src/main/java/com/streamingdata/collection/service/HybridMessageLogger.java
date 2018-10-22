package com.streamingdata.collection.service;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


public class HybridMessageLogger {
    private static RocksDB transientStateDb;
    private static RocksDB failedStateDB;
    private static Options options = null;
    private static Path transientPath = new File("state/transient").toPath();
    private static Path failedPath = new File("state/failed").toPath();

    static void initialize() throws Exception {
        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);

        try {
            ensureDirectory();
            transientStateDb = RocksDB.open(options, transientPath.toString());
            failedStateDB = RocksDB.open(options, failedPath.toString());
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception(e);
        }


    }

    private static void ensureDirectory() throws IOException {
        if (Files.notExists(transientPath)) {
            Files.createDirectories(transientPath);
        }
        if (Files.notExists(failedPath)) {
            Files.createDirectories(failedPath);
        }
    }

    public static void addEvent(String messageKey, byte[] messagePayload) {
        byte[] keyBytes = messageKey.getBytes();
        try {
            byte[] value = transientStateDb.get(keyBytes);
            if (value == null) {
               transientStateDb.put(keyBytes, messagePayload);
            }
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void removeEvent(String messageKey) {
        try {
            transientStateDb.remove(messageKey.getBytes());
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }
    }


    public static void moveToFailed(String eventKey) {
        try {
            byte[] keyBytes = eventKey.getBytes();
            byte[] value = transientStateDb.get(keyBytes);
            if (value != null) {
                failedStateDB.put(keyBytes, value);
                transientStateDb.remove(keyBytes);
            }

        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }

    }

    public static void close() {
        if (transientStateDb != null) {
            transientStateDb.close();
            transientStateDb = null;
        }
        if (failedStateDB != null) {
            failedStateDB.close();
            failedStateDB = null;
        }

        if (options != null) {
            options.close();
            options = null;
        }
    }
}
