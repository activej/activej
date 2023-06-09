/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.fs.util;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.collection.Try;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnexpectedDataException;
import io.activej.common.ref.RefLong;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.process.transformer.ChannelConsumerTransformer;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.*;
import io.activej.fs.tcp.messaging.FileSystemRequest;
import io.activej.fs.tcp.messaging.FileSystemRequest.*;
import io.activej.fs.tcp.messaging.FileSystemResponse;
import io.activej.fs.tcp.messaging.Version;
import io.activej.promise.Promise;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.serializer.stream.StreamCodecs.SubtypeStreamCodec;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class RemoteFileSystemUtils {
	private static final StreamCodec<Version> VERSION_CODEC = StreamCodec.create(Version::new,
		Version::major, StreamCodecs.ofVarInt(),
		Version::minor, StreamCodecs.ofVarInt()
	);
	private static final StreamCodec<FileMetadata> FILE_METADATA_CODEC = StreamCodec.create(FileMetadata::of,
		FileMetadata::getSize, StreamCodecs.ofVarLong(),
		FileMetadata::getTimestamp, StreamCodecs.ofVarLong()
	);
	private static final Pattern ANY_GLOB_METACHARS = Pattern.compile("[*?{}\\[\\]\\\\]");
	private static final Pattern UNESCAPED_GLOB_METACHARS = Pattern.compile("(?<!\\\\)(?:\\\\\\\\)*[*?{}\\[\\]]");

	public static final StreamCodec<FileSystemRequest> FS_REQUEST_CODEC = createFileSystemRequestStreamCodec();
	public static final StreamCodec<FileSystemResponse> FS_RESPONSE_CODEC = createFileSystemResponseStreamCodec();

	/**
	 * Escapes any glob metacharacters so that given path string can ever only match one file.
	 *
	 * @param path path that potentially can contain glob metachars
	 * @return escaped glob which matches only a file with that name
	 */
	public static String escapeGlob(String path) {
		return ANY_GLOB_METACHARS.matcher(path).replaceAll("\\\\$0");
	}

	/**
	 * Checks if given glob can match more than one file.
	 *
	 * @param glob the glob to check.
	 * @return <code>true</code> if given glob can match more than one file.
	 */
	public static boolean isWildcard(String glob) {
		return UNESCAPED_GLOB_METACHARS.matcher(glob).find();
	}

	/**
	 * Returns a {@link PathMatcher} for given glob
	 *
	 * @param glob a glob string
	 * @return a path matcher for the glob string
	 */
	public static PathMatcher getGlobPathMatcher(String glob) {
		return FileSystems.getDefault().getPathMatcher("glob:" + glob);
	}

	/**
	 * Same as {@link #getGlobPathMatcher(String)} but returns a string predicate.
	 *
	 * @param glob a glob string
	 * @return a predicate for the glob string
	 */
	public static Predicate<String> getGlobStringPredicate(String glob) {
		PathMatcher matcher = getGlobPathMatcher(glob);
		return str -> matcher.matches(Paths.get(str));
	}

	public static ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> ofFixedSize(long size) {
		return consumer -> {
			RefLong total = new RefLong(size);
			return consumer
				.<ByteBuf>mapAsync(byteBuf -> {
					long left = total.dec(byteBuf.readRemaining());
					if (left < 0) {
						byteBuf.recycle();
						return Promise.ofException(new UnexpectedDataException());
					}
					return Promise.of(byteBuf);
				})
				.withAcknowledgement(ack -> ack
					.whenResult(() -> {
						if (total.get() > 0) {
							throw new TruncatedDataException();
						}
					}));
		};
	}

	/*
		Casting received errors before sending to remote peer
	 */
	public static FileSystemException castError(Exception e) {
		return e instanceof FileSystemException ? (FileSystemException) e : new FileSystemIOException("Unknown error");
	}

	public static FileSystemBatchException batchException(String name, FileSystemScalarException exception) {
		return new FileSystemBatchException(Map.of(name, exception));
	}

	public static void reduceErrors(List<Try<Void>> tries, Iterator<String> sources) throws Exception {
		Map<String, FileSystemScalarException> scalarExceptions = new HashMap<>();
		for (Try<Void> aTry : tries) {
			String source = sources.next();
			if (aTry.isSuccess()) continue;
			Exception exception = aTry.getException();
			if (exception instanceof FileSystemScalarException) {
				scalarExceptions.put(source, ((FileSystemScalarException) exception));
			} else {
				//noinspection ConstantConditions - a Try is not successfull, hence exception is not 'null'
				throw exception;
			}
		}
		if (!scalarExceptions.isEmpty()) {
			throw new FileSystemBatchException(scalarExceptions);
		}
	}

	private static StreamCodec<FileSystemRequest> createFileSystemRequestStreamCodec() {
		return SubtypeStreamCodec.<FileSystemRequest>builder()
			.withSubtype(FileSystemRequest.Append.class, StreamCodec.create(Append::new,
				Append::name, StreamCodecs.ofString(),
				Append::offset, StreamCodecs.ofVarLong()))
			.withSubtype(FileSystemRequest.Copy.class, StreamCodec.create(Copy::new,
				Copy::name, StreamCodecs.ofString(),
				Copy::target, StreamCodecs.ofString()))
			.withSubtype(FileSystemRequest.CopyAll.class, StreamCodec.create(CopyAll::new,
				CopyAll::sourceToTarget, StreamCodecs.ofMap(StreamCodecs.ofString(), StreamCodecs.ofString())))
			.withSubtype(FileSystemRequest.Delete.class, StreamCodec.create(Delete::new,
				Delete::name, StreamCodecs.ofString()))
			.withSubtype(FileSystemRequest.DeleteAll.class, StreamCodec.create(DeleteAll::new,
				DeleteAll::toDelete, StreamCodecs.ofSet(StreamCodecs.ofString())))
			.withSubtype(FileSystemRequest.Download.class, StreamCodec.create(Download::new,
				Download::name, StreamCodecs.ofString(),
				Download::offset, StreamCodecs.ofVarLong(),
				Download::limit, StreamCodecs.ofVarLong()))
			.withSubtype(FileSystemRequest.Handshake.class, StreamCodec.create(Handshake::new,
				Handshake::version, VERSION_CODEC))
			.withSubtype(FileSystemRequest.Info.class, StreamCodec.create(Info::new,
				Info::name, StreamCodecs.ofString()))
			.withSubtype(FileSystemRequest.InfoAll.class, StreamCodec.create(InfoAll::new,
				InfoAll::names, StreamCodecs.ofSet(StreamCodecs.ofString())))
			.withSubtype(FileSystemRequest.List.class, StreamCodec.create(FileSystemRequest.List::new,
				FileSystemRequest.List::glob, StreamCodecs.ofString()))
			.withSubtype(FileSystemRequest.Move.class, StreamCodec.create(Move::new,
				Move::name, StreamCodecs.ofString(),
				Move::target, StreamCodecs.ofString()))
			.withSubtype(FileSystemRequest.MoveAll.class, StreamCodec.create(MoveAll::new,
				MoveAll::sourceToTarget, StreamCodecs.ofMap(StreamCodecs.ofString(), StreamCodecs.ofString())))
			.withSubtype(FileSystemRequest.Ping.class, StreamCodecs.singleton(new Ping()))
			.withSubtype(FileSystemRequest.Upload.class, StreamCodec.create(Upload::new,
				Upload::name, StreamCodecs.ofString(),
				Upload::size, StreamCodecs.ofVarLong()))
			.build();
	}

	private static StreamCodec<FileSystemResponse> createFileSystemResponseStreamCodec() {
		return SubtypeStreamCodec.<FileSystemResponse>builder()
			.withSubtype(FileSystemResponse.AppendAck.class, StreamCodecs.singleton(new FileSystemResponse.AppendAck()))
			.withSubtype(FileSystemResponse.AppendFinished.class, StreamCodecs.singleton(new FileSystemResponse.AppendFinished()))
			.withSubtype(FileSystemResponse.CopyAllFinished.class, StreamCodecs.singleton(new FileSystemResponse.CopyAllFinished()))
			.withSubtype(FileSystemResponse.CopyFinished.class, StreamCodecs.singleton(new FileSystemResponse.CopyFinished()))
			.withSubtype(FileSystemResponse.DeleteAllFinished.class, StreamCodecs.singleton(new FileSystemResponse.DeleteAllFinished()))
			.withSubtype(FileSystemResponse.DeleteFinished.class, StreamCodecs.singleton(new FileSystemResponse.DeleteFinished()))
			.withSubtype(FileSystemResponse.DownloadSize.class, StreamCodec.create(FileSystemResponse.DownloadSize::new,
				FileSystemResponse.DownloadSize::size, StreamCodecs.ofVarLong()))
			.withSubtype(FileSystemResponse.Handshake.class, StreamCodec.create(FileSystemResponse.Handshake::new,
					FileSystemResponse.Handshake::handshakeFailure, StreamCodecs.ofNullable(
						StreamCodec.create(FileSystemResponse.HandshakeFailure::new,
							FileSystemResponse.HandshakeFailure::minimalVersion, VERSION_CODEC,
							FileSystemResponse.HandshakeFailure::message, StreamCodecs.ofString()))))
			.withSubtype(FileSystemResponse.InfoAllFinished.class, StreamCodec.create(FileSystemResponse.InfoAllFinished::new,
				FileSystemResponse.InfoAllFinished::files, StreamCodecs.ofMap(StreamCodecs.ofString(), FILE_METADATA_CODEC)))
			.withSubtype(FileSystemResponse.InfoFinished.class, StreamCodec.create(FileSystemResponse.InfoFinished::new,
				FileSystemResponse.InfoFinished::fileMetadata, StreamCodecs.ofNullable(FILE_METADATA_CODEC)))
			.withSubtype(FileSystemResponse.ListFinished.class, StreamCodec.create(FileSystemResponse.ListFinished::new,
				FileSystemResponse.ListFinished::files, StreamCodecs.ofMap(StreamCodecs.ofString(), FILE_METADATA_CODEC)))
			.withSubtype(FileSystemResponse.MoveAllFinished.class, StreamCodecs.singleton(new FileSystemResponse.MoveAllFinished()))
			.withSubtype(FileSystemResponse.MoveFinished.class, StreamCodecs.singleton(new FileSystemResponse.MoveFinished()))
			.withSubtype(FileSystemResponse.Pong.class, StreamCodecs.singleton(new FileSystemResponse.Pong()))
			.withSubtype(FileSystemResponse.ServerError.class, StreamCodec.create(FileSystemResponse.ServerError::new,
				FileSystemResponse.ServerError::exception, FileSystemExceptionStreamCodec.createFileSystemExceptionCodec()))
			.withSubtype(FileSystemResponse.UploadAck.class, StreamCodecs.singleton(new FileSystemResponse.UploadAck()))
			.withSubtype(FileSystemResponse.UploadFinished.class, StreamCodecs.singleton(new FileSystemResponse.UploadFinished()))
			.build();
	}
}
