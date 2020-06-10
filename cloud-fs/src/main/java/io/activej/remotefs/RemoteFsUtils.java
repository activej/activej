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

package io.activej.remotefs;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.codec.StructuredCodec;
import io.activej.codec.json.JsonUtils;
import io.activej.common.exception.UncheckedException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.Nullable;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.activej.remotefs.FsClient.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;

public final class RemoteFsUtils {
	private static final Pattern ANY_GLOB_METACHARS = Pattern.compile("[*?{}\\[\\]\\\\]");
	private static final Pattern UNESCAPED_GLOB_METACHARS = Pattern.compile("(?<!\\\\)(?:\\\\\\\\)*[*?{}\\[\\]]");

	public static final List<Throwable> KNOWN_ERRORS = unmodifiableList(Arrays.asList(
			FILE_NOT_FOUND,
			FILE_EXISTS,
			BAD_PATH,
			OFFSET_TOO_BIG,
			LENGTH_TOO_BIG,
			BAD_RANGE,
			IS_DIRECTORY,
			UNSUPPORTED_REVISION
	));

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

	public static int getErrorCode(Throwable e) {
		if (e == FILE_NOT_FOUND) {
			return 1;
		}
		if (e == FILE_EXISTS) {
			return 2;
		}
		if (e == BAD_PATH) {
			return 3;
		}
		if (e == OFFSET_TOO_BIG) {
			return 4;
		}
		if (e == LENGTH_TOO_BIG) {
			return 5;
		}
		if (e == BAD_RANGE) {
			return 6;
		}
		if (e == IS_DIRECTORY) {
			return 7;
		}
		if (e == UNSUPPORTED_REVISION) {
			return 8;
		}
		return 0;
	}

	static void checkRange(long size, long offset, long length) {
		if (offset < -1 || length < -1) {
			throw new UncheckedException(BAD_RANGE);
		}
		if (offset > size) {
			throw new UncheckedException(OFFSET_TOO_BIG);
		}
		if (length != -1 && offset + length > size) {
			throw new UncheckedException(LENGTH_TOO_BIG);
		}
	}

	public static Promise<Void> copyFile(FsClient source, FsClient target, String name) {
		return copyFile(source, target, name, null);
	}

	public static Promise<Void> copyFile(FsClient from, FsClient to, String name, @Nullable Long newRevision) {
		return Promises.toTuple(from.getMetadata(name), to.getMetadata(name))
				.then(t -> {
					FileMetadata sourceMeta = t.getValue1();
					FileMetadata targetMeta = t.getValue2();

					if (sourceMeta == null) {
						// no such original file, do nothing
						return Promise.complete();
					}

					long sourceRevision = newRevision != null ? newRevision : sourceMeta.getRevision();

					if (sourceMeta.isTombstone()) {
						if (targetMeta != null && sourceRevision < targetMeta.getRevision()) {
							// target meta is better than our tombstone, do nothing
							return Promise.complete();
						}
						// else create the same tombstone on target
						return to.delete(name, sourceRevision);
					}

					if (targetMeta == null || sourceRevision > targetMeta.getRevision()) {
						// simply copy over when target has no such file or when source file is better
						return from.download(name)
								.then(supplier -> supplier.streamTo(to.upload(name, sourceRevision)));
					}

					if (sourceRevision < targetMeta.getRevision()) {
						// do nothing when target file is better
						return Promise.complete();
					}

					// * the revisions are equal here

					if (sourceMeta.getSize() <= targetMeta.getSize()) {
						// if target is the same or bigger then it is better, do nothing
						return Promise.complete();
					}

					// else we copy over only the part that is missing on target
					return from.download(name, sourceMeta.getSize())
							.then(supplier -> supplier.streamTo(to.upload(name, sourceRevision)));
				});
	}

	static <I, O> ByteBufsCodec<I, O> nullTerminatedJson(StructuredCodec<I> in, StructuredCodec<O> out) {
		return ByteBufsCodec
				.ofDelimiter(
						ByteBufsDecoder.ofNullTerminatedBytes(),
						buf -> {
							ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
							buf1.put((byte) 0);
							return buf1;
						})
				.andThen(
						buf -> JsonUtils.fromJson(in, buf.asString(UTF_8)),
						item -> JsonUtils.toJsonBuf(out, item));
	}

}
