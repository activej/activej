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

import org.intellij.lang.annotations.Language;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is an util class, which provides a mean to render any exception into an {@link HttpResponse}
 * with the stacktrace rendered nicely.
 * It also generates a link to the IntelliJ IDEA REST API (http://localhost:63342 + port_offset) from the stacktrace,
 * just like IDEA does in its log console.
 */
public final class DebugStacktraceRenderer {
	private static final String IDEA_REST_API_STARTING_PORT = "63342";

	@Language("HTML")
	private static final String DEBUG_SERVER_ERROR_HTML = "<!doctype html>" +
			"<html lang=\"en\">" +
			"<head>" +
			"<meta charset=\"UTF-8\">" +
			"<title>{title}</title>" +
			"<style>" +
			"html, body { height: 100%; margin: 0; padding: 0; }" +
			"h1, p { font-family: sans-serif; }" +
			".link { color: #00E; text-decoration: underline; cursor: pointer; }" +
			"</style>" +
			"</head>" +
			"<body>" +
			"<script>" +
			"window.onload = () => {" +
			"function check(portOffset) {" +
			"if (portOffset > 10) return Promise.reject('no running intellij idea instance found');" +
			"return fetch('http://localhost:' + (" + IDEA_REST_API_STARTING_PORT + " + portOffset))" +
			".then(r => r.text())" +
			".then(t => t.includes('IDEA') ? (" + IDEA_REST_API_STARTING_PORT + " + portOffset) : check(portOffset + 1), () => check(portOffset + 1));" +
			"}" +
			"check(0).then(port => document.querySelectorAll('[data-target]').forEach(a => {" +
			"a.onclick = () => fetch('http://localhost:' + port + '/' + a.dataset.target);" +
			"a.classList.add('link');" +
			"}));};" +
			"</script>" +
			"<div style=\"position:relative;min-height:100%;\">" +
			"<h1 style=\"text-align:center;margin-top:0;padding-top:0.5em;\">{title}</h1>" +
			"<hr style=\"margin-left:10px;margin-right:10px;\">" +
			"<pre style=\"color:#8B0000;font-size:1.5em;padding:10px 10px 4em;\">{stacktrace}</pre>" +
			"<div style=\"position:absolute;bottom:1px;width:100%;height:4em\">" +
			"<hr style=\"margin-left:10px;margin-right:10px\">" +
			"<p style=\"text-align:center;\">ActiveJ " + HttpExceptionFormatter.ACTIVEJ_VERSION + "</p>" +
			"</div>" +
			"</div>" +
			"</body>" +
			"</html>";

	private static final Pattern STACK_TRACE_ELEMENT;

	static {
		String ident = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
		STACK_TRACE_ELEMENT = Pattern.compile("(at ((?:" + ident + "\\.)+)" + ident + "\\()(" + ident + "(\\." + ident + ")(:\\d+)?)\\)");
	}

	public static HttpResponse render(Exception e, int code) {
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));
		Matcher matcher = STACK_TRACE_ELEMENT.matcher(writer.toString());
		StringBuilder stacktrace = new StringBuilder();
		while (matcher.find()) {
			String cls = matcher.group(2);
			String quotedFile = Matcher.quoteReplacement(cls.substring(0, cls.length() - 1).replace('.', '/').replaceAll("\\$.*(?:\\.|$)", ""));
			matcher.appendReplacement(stacktrace, "$1<a data-target=\"api/file/" + quotedFile + "$4$5\">$3</a>)");
		}
		matcher.appendTail(stacktrace);
		return HttpResponse.ofCode(code)
				.withHtml(DEBUG_SERVER_ERROR_HTML
						.replace("{title}", HttpUtils.getHttpErrorTitle(code))
						.replace("{stacktrace}", stacktrace));
	}
}
