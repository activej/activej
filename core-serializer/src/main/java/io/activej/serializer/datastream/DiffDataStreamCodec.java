package io.activej.serializer.datastream;

public interface DiffDataStreamCodec<T> extends DataStreamCodec<T>, DiffDataStreamEncoder<T>, DiffDataStreamDecoder<T> {
}
