module io.activej.net {
	requires transitive io.activej.bytebuf;
	requires transitive io.activej.promise;

	exports io.activej.net;
	exports io.activej.net.socket.tcp;
	exports io.activej.net.socket.udp;
}
