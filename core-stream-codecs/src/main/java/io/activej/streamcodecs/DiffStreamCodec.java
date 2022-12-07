package io.activej.streamcodecs;

public interface DiffStreamCodec<T> extends StreamCodec<T>, DiffStreamEncoder<T>, DiffStreamDecoder<T> {
}
