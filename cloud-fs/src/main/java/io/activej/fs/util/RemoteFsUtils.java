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
import io.activej.csp.ChannelConsumer;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.*;
import io.activej.fs.tcp.messaging.FsRequest;
import io.activej.fs.tcp.messaging.FsRequest.*;
import io.activej.fs.tcp.messaging.FsResponse;
import io.activej.fs.tcp.messaging.Version;
import io.activej.promise.Promise;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class RemoteFsUtils {
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

	public static final StreamCodec<FsRequest> FS_REQUEST_CODEC = createFsRequestStreamCodec();
	public static final StreamCodec<FsResponse> FS_RESPONSE_CODEC = createFsResponseStreamCodec();

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
							.whenResult($ -> total.get() > 0, () -> {
								throw new TruncatedDataException();
							}));
		};
	}

	/*
		Casting received errors before sending to remote peer
	 */
	public static FsException castError(Exception e) {
		return e instanceof FsException ? (FsException) e : new FsIOException("Unknown error");
	}

	public static FsBatchException fsBatchException(String name, FsScalarException exception) {
		return new FsBatchException(Map.of(name, exception));
	}

	public static void reduceErrors(List<Try<Void>> tries, Iterator<String> sources) throws Exception {
		Map<String, FsScalarException> scalarExceptions = new HashMap<>();
		for (Try<Void> aTry : tries) {
			String source = sources.next();
			if (aTry.isSuccess()) continue;
			Exception exception = aTry.getException();
			if (exception instanceof FsScalarException) {
				scalarExceptions.put(source, ((FsScalarException) exception));
			} else {
				//noinspection ConstantConditions - a Try is not successfull, hence exception is not 'null'
				throw exception;
			}
		}
		if (!scalarExceptions.isEmpty()) {
			throw new FsBatchException(scalarExceptions);
		}
	}

	private static StreamCodec<FsRequest> createFsRequestStreamCodec() {
		StreamCodecs.SubtypeBuilder<FsRequest> builder = new StreamCodecs.SubtypeBuilder<>();

		builder.add(FsRequest.Append.class, StreamCodec.create(Append::new,
				Append::name, StreamCodecs.ofString(),
				Append::offset, StreamCodecs.ofVarLong())
		);
		builder.add(FsRequest.Copy.class, StreamCodec.create(Copy::new,
				Copy::name, StreamCodecs.ofString(),
				Copy::target, StreamCodecs.ofString())
		);
		builder.add(FsRequest.CopyAll.class, StreamCodec.create(CopyAll::new,
				CopyAll::sourceToTarget, StreamCodecs.ofMap(StreamCodecs.ofString(), StreamCodecs.ofString()))
		);
		builder.add(FsRequest.Delete.class, StreamCodec.create(Delete::new,
				Delete::name, StreamCodecs.ofString())
		);
		builder.add(FsRequest.DeleteAll.class, StreamCodec.create(DeleteAll::new,
				DeleteAll::toDelete, StreamCodecs.ofSet(StreamCodecs.ofString()))
		);
		builder.add(FsRequest.Download.class, StreamCodec.create(Download::new,
				Download::name, StreamCodecs.ofString(),
				Download::offset, StreamCodecs.ofVarLong(),
				Download::limit, StreamCodecs.ofVarLong())
		);
		builder.add(FsRequest.Handshake.class, StreamCodec.create(Handshake::new,
				Handshake::version, VERSION_CODEC)
		);
		builder.add(FsRequest.Info.class, StreamCodec.create(Info::new,
				Info::name, StreamCodecs.ofString())
		);
		builder.add(FsRequest.InfoAll.class, StreamCodec.create(InfoAll::new,
				InfoAll::names, StreamCodecs.ofSet(StreamCodecs.ofString()))
		);
		builder.add(FsRequest.List.class, StreamCodec.create(FsRequest.List::new,
				FsRequest.List::glob, StreamCodecs.ofString())
		);
		builder.add(FsRequest.Move.class, StreamCodec.create(Move::new,
				Move::name, StreamCodecs.ofString(),
				Move::target, StreamCodecs.ofString())
		);
		builder.add(FsRequest.MoveAll.class, StreamCodec.create(MoveAll::new,
				MoveAll::sourceToTarget, StreamCodecs.ofMap(StreamCodecs.ofString(), StreamCodecs.ofString()))
		);
		builder.add(FsRequest.Ping.class, StreamCodecs.singleton(new Ping()));
		builder.add(FsRequest.Upload.class, StreamCodec.create(Upload::new,
				Upload::name, StreamCodecs.ofString(),
				Upload::size, StreamCodecs.ofVarLong())
		);

		return builder.build();
	}

	private static StreamCodec<FsResponse> createFsResponseStreamCodec() {
		StreamCodecs.SubtypeBuilder<FsResponse> builder = new StreamCodecs.SubtypeBuilder<>();

		builder.add(FsResponse.AppendAck.class, StreamCodecs.singleton(new FsResponse.AppendAck()));
		builder.add(FsResponse.AppendFinished.class, StreamCodecs.singleton(new FsResponse.AppendFinished()));
		builder.add(FsResponse.CopyAllFinished.class, StreamCodecs.singleton(new FsResponse.CopyAllFinished()));
		builder.add(FsResponse.CopyFinished.class, StreamCodecs.singleton(new FsResponse.CopyFinished()));
		builder.add(FsResponse.DeleteAllFinished.class, StreamCodecs.singleton(new FsResponse.DeleteAllFinished()));
		builder.add(FsResponse.DeleteFinished.class, StreamCodecs.singleton(new FsResponse.DeleteFinished()));
		builder.add(FsResponse.DownloadSize.class, StreamCodec.create(FsResponse.DownloadSize::new,
				FsResponse.DownloadSize::size, StreamCodecs.ofVarLong())
		);
		builder.add(FsResponse.Handshake.class, StreamCodec.create(FsResponse.Handshake::new,
						FsResponse.Handshake::handshakeFailure, StreamCodecs.ofNullable(
								StreamCodec.create(FsResponse.HandshakeFailure::new,
										FsResponse.HandshakeFailure::minimalVersion, VERSION_CODEC,
										FsResponse.HandshakeFailure::message, StreamCodecs.ofString())
						)
				)
		);
		builder.add(FsResponse.InfoAllFinished.class, StreamCodec.create(FsResponse.InfoAllFinished::new,
				FsResponse.InfoAllFinished::files, StreamCodecs.ofMap(StreamCodecs.ofString(), FILE_METADATA_CODEC))
		);
		builder.add(FsResponse.InfoFinished.class, StreamCodec.create(FsResponse.InfoFinished::new,
				FsResponse.InfoFinished::fileMetadata, StreamCodecs.ofNullable(FILE_METADATA_CODEC))
		);
		builder.add(FsResponse.ListFinished.class, StreamCodec.create(FsResponse.ListFinished::new,
				FsResponse.ListFinished::files, StreamCodecs.ofMap(StreamCodecs.ofString(), FILE_METADATA_CODEC))
		);
		builder.add(FsResponse.MoveAllFinished.class, StreamCodecs.singleton(new FsResponse.MoveAllFinished()));
		builder.add(FsResponse.MoveFinished.class, StreamCodecs.singleton(new FsResponse.MoveFinished()));
		builder.add(FsResponse.Pong.class, StreamCodecs.singleton(new FsResponse.Pong()));
		builder.add(FsResponse.ServerError.class, StreamCodec.create(FsResponse.ServerError::new,
				FsResponse.ServerError::exception, FsExceptionStreamCodec.createFsExceptionCodec()));
		builder.add(FsResponse.UploadAck.class, StreamCodecs.singleton(new FsResponse.UploadAck()));
		builder.add(FsResponse.UploadFinished.class, StreamCodecs.singleton(new FsResponse.UploadFinished()));

		return builder.build();
	}
}
