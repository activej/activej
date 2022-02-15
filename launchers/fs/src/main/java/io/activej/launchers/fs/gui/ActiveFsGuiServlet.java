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

package io.activej.launchers.fs.gui;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.util.ByteBufWriter;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.ref.Ref;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FsException;
import io.activej.fs.http.ActiveFsServlet;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.http.RoutingServlet;
import io.activej.promise.Promise;

import java.util.*;

import static io.activej.common.Utils.mapOf;
import static io.activej.common.Utils.not;
import static io.activej.fs.http.FsCommand.DOWNLOAD;
import static io.activej.fs.http.FsCommand.UPLOAD;
import static io.activej.http.ContentTypes.HTML_UTF_8;
import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.CONTENT_TYPE;
import static io.activej.http.HttpMethod.POST;
import static io.activej.http.HttpResponse.redirect302;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public final class ActiveFsGuiServlet implements WithInitializer<ActiveFsGuiServlet> {
	private static final String HIDDEN_FILE = ".EMPTY";

	private ActiveFsGuiServlet() {
	}

	public static RoutingServlet create(ActiveFs fs) {
		return create(fs, "ActiveJ FS");
	}

	public static RoutingServlet create(ActiveFs fs, String title) {
		Mustache mustache = new DefaultMustacheFactory().compile("fs/gui/static/index.html");
		RoutingServlet fsServlet = ActiveFsServlet.create(fs);

		RoutingServlet uploadServlet = fsServlet.getSubtree("/" + UPLOAD);
		RoutingServlet downloadServlet = fsServlet.getSubtree("/" + DOWNLOAD);
		assert uploadServlet != null && downloadServlet != null;

		return RoutingServlet.create()
				.map("/api/upload", uploadServlet)
				.map("/api/download/*", downloadServlet)
				.map(POST, "/api/newDir", request -> request.loadBody()
						.then(() -> {
							String dir = request.getPostParameter("dir");
							if (dir == null || dir.isEmpty())
								return Promise.of(HttpResponse.ofCode(400).withPlainText("Dir should not be empty"));
							return ChannelSupplier.<ByteBuf>of().streamTo(fs.upload(dir + "/" + HIDDEN_FILE))
									.map($ -> HttpResponse.ok200());
						}))
				.map("/", request -> {
					String dir = decodeDir(request);
					return fs.list(dir + "**")
							.map(
									files -> !dir.isEmpty() && files.isEmpty() ?
											redirect302("/") :
											HttpResponse.ok200()
													.withHeader(CONTENT_TYPE, ofContentType(HTML_UTF_8))
													.withBody(applyTemplate(mustache, mapOf(
															"title", title,
															"dirContents", filesToDirView(new HashMap<>(files), dir),
															"breadcrumbs", dirToBreadcrumbs(dir)))),
									e -> {
										if (e instanceof FsException) {
											return HttpResponse.ofCode(500)
													.withPlainText("Service unavailable");
										} else {
											throw e;
										}
									});
				})
				.map("/*", $ -> HttpResponse.notFound404());
	}

	private static ByteBuf applyTemplate(Mustache mustache, Map<String, Object> scopes) {
		ByteBufWriter writer = new ByteBufWriter();
		mustache.execute(writer, scopes);
		return writer.getBuf();
	}

	private static String decodeDir(HttpRequest request) {
		String dir = request.getQueryParameter("dir");
		if (dir == null) return "";
		dir = dir.replaceAll("^/+", "");
		return dir.isEmpty() ? dir : dir + '/';
	}

	private static DirView filesToDirView(Map<String, FileMetadata> files, String currentDir) {
		files.keySet().removeIf(s -> !s.startsWith(currentDir));

		Set<Dir> dirs = new TreeSet<>(comparing(Dir::getShortName));
		Set<FileView> fileViews = new TreeSet<>(comparing(FileView::getName));
		for (Map.Entry<String, FileMetadata> entry : files.entrySet()) {
			String name = entry.getKey();
			FileMetadata meta = entry.getValue();
			int slashIdx = name.indexOf('/', currentDir.length());
			if (slashIdx == -1) {
				String shortName = name.substring(currentDir.length());
				if (shortName.equals(HIDDEN_FILE)) continue;
				fileViews.add(new FileView(shortName, name, meta.getSize(), meta.getTimestamp()));
			} else {
				dirs.add(new Dir(name.substring(currentDir.length(), slashIdx), name.substring(0, slashIdx)));
			}
		}

		return new DirView(currentDir, dirs, fileViews);
	}

	private static List<Dir> dirToBreadcrumbs(String dir) {
		Ref<String> fullPath = new Ref<>("");
		return Arrays.stream(dir.split("/+"))
				.map(String::trim)
				.filter(not(String::isEmpty))
				.map(pathPart -> new Dir(pathPart, fullPath.value += (fullPath.value.isEmpty() ? "" : '/') + pathPart))
				.collect(toList());
	}

}
