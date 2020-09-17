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

package io.activej.http;

import io.activej.bytebuf.ByteBuf;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a collection of most well-known {@link MediaType} token references as Java constants.
 */
@SuppressWarnings({"unused", "SameParameterValue"})
public final class MediaTypes {
	static final CaseInsensitiveTokenMap<MediaType> mimes = new CaseInsensitiveTokenMap<>(2048, 2, MediaType.class, MediaType::new);
	private static final Map<String, MediaType> ext2mime = new HashMap<>();

	public static final MediaType ANY = register("*/*");
	public static final MediaType ANY_APPLICATION = register("application/*");
	public static final MediaType ANY_TEXT = register("text/*");
	public static final MediaType ANY_IMAGE = register("image/*");
	public static final MediaType ANY_AUDIO = register("audio/*");
	public static final MediaType ANY_VIDEO = register("video/*");

	public static final MediaType X_WWW_FORM_URLENCODED = register("application/x-www-form-urlencoded");
	public static final MediaType ATOM = register("application/atom+xml", "atom");
	public static final MediaType EDI_X12 = register("application/EDI-X12");
	public static final MediaType EDI_EDIFACT = register("application/EDIFACT");
	public static final MediaType JSON = register("application/json", "json");
	public static final MediaType JAVASCRIPT = register("application/javascript", "js");
	public static final MediaType OCTET_STREAM = register("application/octet-stream", "com", "exe", "bin");
	public static final MediaType ZIP = register("application/zip", "zip", "zipx");
	public static final MediaType GZIP = register("application/gzip", "gzip", "gz");
	public static final MediaType BZIP2 = register("application/x-bzip2", "bz2");
	public static final MediaType FLASH = register("application/x-shockwave-flash");
	public static final MediaType TEX = register("application/x-tex");
	public static final MediaType PDF = register("application/pdf", "pdf");
	public static final MediaType OGG_APP = register("application/ogg", "ogg");
	public static final MediaType POSTSCRIPT = register("application/postscript", "ai");
	public static final MediaType BINARY = register("application/binary");
	public static final MediaType TAR = register("application/x-tar", "tar");
	public static final MediaType KEY_ARCHIVE = register("application/pkcs12", "p12", "pfx");
	public static final MediaType PROTOBUF = register("application/protobuf", "proto");
	public static final MediaType EPUB = register("application/epub+zip", "epub");
	public static final MediaType RDF = register("application/rdf+xml", "rdf");
	public static final MediaType XRD = register("application/xrd+xml", "xrd");
	public static final MediaType JAVA_ARCHIVE = register("application/java-archive", "jar", "war", "ear");
	public static final MediaType WOFF = register("application/font-woff", "woff");
	public static final MediaType EOT = register("application/vnd.ms-fontobject", "eot");
	public static final MediaType SFNT = register("application/font-sfnt", "sfnt");
	public static final MediaType XML_APP = register("application/xml");
	public static final MediaType XHTML_APP = register("application/xhtml+xml");

	public static final MediaType CSS = register("text/css", "css");
	public static final MediaType CSV = register("text/csv", "csv");
	public static final MediaType HTML = register("text/html", "html", "htm");
	public static final MediaType PLAIN_TEXT = register("text/plain", "txt");
	public static final MediaType RTF = register("text/rtf", "rtf");
	public static final MediaType XML = register("text/xml", "xml");
	public static final MediaType XHTML = register("text/xhtml+xml", "xhtml");
	public static final MediaType GV = register("text/vnd.graphviz", "gv"); // https://www.iana.org/assignments/media-types/text/vnd.graphviz

	public static final MediaType BMP = register("image/bmp", "bmp");
	public static final MediaType ICO = register("image/vnd.microsoft.icon", "ico");
	public static final MediaType CRW = register("image/x-canon-crw", "crw");
	public static final MediaType PSD = register("image/vnd.adobe.photoshop", "psd");
	public static final MediaType WEBP = register("image/webp", "webp");
	public static final MediaType GIF = register("image/gif", "gif");
	public static final MediaType JPEG = register("image/jpeg", "jpeg", "jpg");
	public static final MediaType PNG = register("image/png", "png");
	public static final MediaType SVG = register("image/svg+xml", "svg");
	public static final MediaType TIFF = register("image/tiff", "tiff", "tif");
	public static final MediaType OGG_AUDIO = register("audio/ogg", "oga");
	public static final MediaType MP4_AUDIO = register("audio/mp4", "mp4a");
	public static final MediaType MPEG_AUDIO = register("audio/mpeg", "mpga");
	public static final MediaType WEBM_AUDIO = register("audio/webm", "weba");
	public static final MediaType FLAC = register("audio/x-flac", "flac");
	public static final MediaType MP3 = register("audio/mp3", "mp3");
	public static final MediaType AAC = register("audio/aac", "aac");
	public static final MediaType WMA = register("audio/x-ms-wma", "wma");
	public static final MediaType REALAUDIO = register("audio/vnd.rn-realaudio", "rm");
	public static final MediaType WAVE = register("audio/vnd.wave", "wave", "wav");

	public static final MediaType MPEG = register("video/mpeg", "mpeg", "mpg");
	public static final MediaType MP4 = register("video/mp4", "mp4");
	public static final MediaType QUICKTIME = register("video/quicktime", "mov", "moov", "qt", "qtvr");
	public static final MediaType OGG_VIDEO = register("video/ogg", "ogv");
	public static final MediaType WEBM = register("video/webm", "webm");
	public static final MediaType WMV = register("video/x-ms-wmv", "wmv");
	public static final MediaType FLV = register("video/x-flv", "flv");
	public static final MediaType AVI = register("video/avi", "avi");

	static MediaType of(byte[] bytes, int offset, int length, int lowerCaseHashCode) {
		return mimes.getOrCreate(bytes, offset, length, lowerCaseHashCode);
	}

	static void render(MediaType mime, ByteBuf buf) {
		int len = render(mime, buf.array(), buf.tail());
		buf.moveTail(len);
	}

	static int render(MediaType mime, byte[] container, int pos) {
		System.arraycopy(mime.bytes, mime.offset, container, pos, mime.length);
		return mime.length;
	}

	public static MediaType getByExtension(String ext) {
		return ext2mime.get(ext);
	}

	private static MediaType register(String name, String... ext) {
		MediaType mime = mimes.register(name);
		bindExts2mime(mime, ext);
		return mime;
	}

	private static void bindExts2mime(MediaType mime, String... exts) {
		for (String ext : exts) {
			ext2mime.put(ext, mime);
		}
	}
}
