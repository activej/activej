[![Maven Central](https://img.shields.io/maven-central/v/io.activej/activej)](https://mvnrepository.com/artifact/io.activej)
[![GitHub](https://img.shields.io/github/license/activej/activej)](https://github.com/activej/activej/blob/master/LICENSE)

## Introduction

[ActiveJ](https://activej.io) is a modern Java platform built from the ground up. 
It is designed to be self-sufficient (no third-party dependencies), simple, lightweight and provides competitive performance. 
ActiveJ consists of a range of libraries, from dependency injection and high-performance
asynchronous I/O (inspired by Node.js), to application servers and big data solutions. You can use ActiveJ to build scalable
web applications, distributed systems and use it for high-load data processing.

## ActiveJ components

ActiveJ consists of several modules, which can be logically grouped into the following categories :

* **Async.io** - High-performance asynchronous IO with the efficient event loop, NIO, promises, streaming, and CSP.
  Alternative to Netty, RxJava, Akka, and others. ([Promise](https://activej.io/async-io/promise),
  [Eventloop](https://activej.io/async-io/eventloop), [Net](https://activej.io/async-io/net),
  [CSP](https://activej.io/async-io/csp), [Datastream](https://activej.io/async-io/datastream))
* **HTTP** - High-performance HTTP server and client with WebSocket support. It can be used as a simple web server or as an
  application server. Alternative to other conventional HTTP clients and servers. ([HTTP](https://activej.io/http))
* **ActiveJ Inject** - Lightweight library for dependency injection. Optimized for fast application start-up and
  performance at runtime. Supports annotation-based component wiring as well as reflection-free
  wiring. ([ActiveJ Inject](https://activej.io/inject))
* **Boot** - Production-ready tools for running and monitoring an ActiveJ application. Concurrent control of services lifecycle 
  based on their dependencies. Various service monitoring utilities with JMX and Zabbix support. ([Launcher](https://activej.io/boot/launcher),
  [Service Graph](https://activej.io/boot/servicegraph), [JMX](https://github.com/activej/activej/tree/master/boot-jmx),
  [Triggers](https://github.com/activej/activej/tree/master/boot-triggers))
* **Bytecode manipulation**
    * **ActiveJ Codegen** - Dynamic bytecode generator for classes and methods on top of [ObjectWeb ASM](https://asm.ow2.io/)
      library. Abstracts the complexity of direct bytecode manipulation and allows you to create custom classes on the fly
      using Lisp-like AST expressions. ([ActiveJ Codegen](https://activej.io/codegen))
    * **ActiveJ Serializer** - [Fast](https://github.com/activej/jvm-serializers) and space-efficient serializers created with bytecode engineering.
      Introduces schema-free approach for best performance. ([ActiveJ Serializer](https://activej.io/serializer))
    * **ActiveJ Specializer** - Innovative technology to improve class performance at runtime by automatically
      converting class instances into specialized static classes and class instance fields into baked-in static
      fields. Provides a wide variety of JVM optimizations for static classes that are impossible otherwise: dead code
      elimination, aggressive inlining of methods and static
      constants. ([ActiveJ Specializer](https://activej.io/specializer))
* **Cloud components**
    * **ActiveJ FS** - Asynchronous abstraction over the file system for building efficient, scalable local or remote file
      storages that support data redundancy, rebalancing, and resharding.
      ([ActiveJ FS](https://activej.io/fs))
    * **ActiveJ RPC** - High-performance binary client-server protocol. Allows building distributed, sharded, and
      fault-tolerant microservice applications. ([ActiveJ RPC](https://activej.io/rpc))
    * Various extra services:
      [ActiveJ CRDT](https://github.com/activej/activej/tree/master/extra/cloud-crdt),
      [Redis client](https://github.com/activej/activej/tree/master/extra/cloud-redis),
      [Memcache](https://github.com/activej/activej/tree/master/extra/cloud-memcache),
      [OLAP Cube](https://github.com/activej/activej/tree/master/extra/cloud-lsmt-cube),
      [Dataflow](https://github.com/activej/activej/tree/master/extra/cloud-dataflow)

## Quick start

Paste this snippet into your terminal...

```
mvn archetype:generate -DarchetypeGroupId=io.activej -DarchetypeArtifactId=archetype-http -DarchetypeVersion=5.2
```

... and open the project in your favorite  IDE. Then build the application and run it. Open your browser
on [localhost:8080](http://localhost:8080)
to see the "Hello World" message.

#### Full-featured embedded web application server with Dependency Injection:

```java
public final class HttpHelloWorldExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return request -> HttpResponse.ok200().withPlainText("Hello, World!");
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpHelloWorldExample();
		launcher.launch(args);
	}
}
```

Some technical details about the example above:

- *The JAR file size is only 1.4 MB. By comparison, the minimum size of a Spring web application is about 17 MB*.
- *The cold start time is 0.65 sec.*
- *The [ActiveJ Inject](https://activej.io/inject) DI library used is 5.5 times faster than Guice and hundreds
  of times faster than Spring.*

To learn more about ActiveJ, please visit https://activej.io or follow our
5-minute [getting-started guide](https://activej.io/tutorials/getting-started).

Examples of using the ActiveJ platform and all ActiveJ libraries can be found
in the [`examples`](https://github.com/activej/activej/tree/master/examples) module.

**Release notes for ActiveJ can be found [here](https://activej.io/blog)**