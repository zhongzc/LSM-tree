package com.gaufoo.sst;

import scala.compat.java8.*;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public class SST {
    final private KVEngine kvEngine;

    private SST(String dbName, Path storingDir) {
        this.kvEngine = SSTEngine.build(dbName, storingDir, 1500);
    }

    public CompletionStage<String> set(String key, String value) {
        return FutureConverters.toJava(kvEngine.set(key, value));
    }

    public CompletionStage<Optional<String>> get(String key) {
        return FutureConverters.toJava(kvEngine.get(key)).thenApply(OptionConverters::toJava);
    }

    public CompletionStage<Optional<String>> delete(String key) {
        return FutureConverters.toJava(kvEngine.delete(key)).thenApply(OptionConverters::toJava);
    }

    public CompletionStage<Stream<String>> allKeysAsc() {
        return FutureConverters.toJava(kvEngine.allKeysAsc()).thenApply(ScalaStreamSupport::stream);
    }

    public CompletionStage<Stream<String>> allKeysDes() {
        return FutureConverters.toJava(kvEngine.allKeysDes()).thenApply(ScalaStreamSupport::stream);
    }

    public void shutdown() {
        kvEngine.shutdown();
    }

    public boolean isShutdown() {
        return kvEngine.isShutdown();
    }

    public static SST of(String dbName, Path path) {
        return new SST(dbName, path);
    }
}
