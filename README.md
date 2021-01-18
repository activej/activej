[![Maven Central](https://img.shields.io/maven-central/v/io.activej/activej)](https://mvnrepository.com/artifact/io.activej)
[![GitHub](https://img.shields.io/github/license/activej/activej)](https://github.com/activej/activej/blob/master/LICENSE)

## Introduction

[ActiveJ](https://activej.io) is a fully-featured alternative Java platform built from the ground up as a replacement of
Spring, Spark, Quarkus, Micronauts, and other solutions. It is minimalistic, boilerplate-free, and incomparably faster,
which is proven by benchmarks. ActiveJ has very few third-party dependencies, yet features a rich stack of technologies
with an efficient async programming model and a powerful DI library [ActiveJ Inject](https://inject.activej.io)

## ActiveJ components

ActiveJ consists of several modules that can be logically grouped into following categories :

* **Async.io** - High-performance asynchronous IO with efficient event loop, NIO, promises,
  streaming and CSP. Alternative to Netty, RxJava, Akka and others. ([Promise](https://activej.io/promise),
  [Eventloop](https://activej.io/eventloop), [Net](https://activej.io/net),
  [CSP](https://activej.io/csp), [Datastream](https://activej.io/datastream))
* **HTTP** - High-performance HTTP server and client with WebSocket support. Can be used as a simple web server or as an
  application server. An alternative to Jetty and other conventional HTTP clients and
  servers. ([HTTP](https://activej.io/http))
* **ActiveJ Inject** - Lightweight powerful dependency injection library. Optimized for fast application start-up and
  ultimate runtime performance. Supports annotation-based component wiring as well as reflection-free
  wiring. ([ActiveJ Inject](https://inject.activej.io/))
* **Boot** - Production-ready tools for launching and monitoring ActiveJ application.
  Concurrently starts and stops services based on their dependencies. Various service monitoring
  utilities with the support of JMX and Zabbix. ([Launcher](https://activej.io/launcher), 
  [Service Graph](https://activej.io/service-graph), [JMX](https://github.com/activej/activej/tree/master/boot-jmx), 
  [Triggers](https://github.com/activej/activej/tree/master/boot-triggers))
* **Bytecode manipulation**
    * **ActiveJ Codegen** - Dynamic class and method bytecode generator on top of [ObjectWeb ASM](https://asm.ow2.io/)
      library. Abstracts the complexity of direct bytecode manipulation and allows to create custom classes on the
      fly using Lisp-like AST expressions. ([ActiveJ Codegen](https://codegen.activej.io/))
    * **ActiveJ Serializer** - Extremely fast and space-efficient serializers created with bytecode engineering.
      Introduces schema-less approach for the best performance. ([ActiveJ Serializer](https://serializer.activej.io/))
    * **ActiveJ Specializer** - Innovative technology for enhancing class runtime performance by automatically 
      transforming class instances into specialized static classes, and class
      instance fields into baked-in static fields. Enables a wide variety of JVM optimizations for
      static classes, not possible otherwise: dead code elimination, aggressively inlining
      methods, and static constants. ([ActiveJ Specializer](https://specializer.activej.io/))
* **Cloud components**
    * **ActiveJ FS** - Asynchronous abstraction over file system for building efficient, scalable local or remote 
      file storages, supporting data redundancy, rebalancing, and resharding.
      ([ActiveJ FS](https://fs.activej.io/))
    * **ActiveJ RPC** - Ultra high-performance binary client-server protocol. Allows to build distributed, 
      sharded and fault-tolerant microservices applications. ([ActiveJ RPC](https://rpc.activej.io/))
    * Various extra services:
      [ActiveJ CRDT](https://crdt.activej.io/),
      [Redis client](https://github.com/activej/activej/tree/master/extra/cloud-redis),
      [Memcache](https://github.com/activej/activej/tree/master/extra/cloud-memcache),
      [OLAP Cube](https://github.com/activej/activej/tree/master/extra/cloud-lsmt-cube),
      [Dataflow](https://github.com/activej/activej/tree/master/extra/cloud-dataflow)

## Quick start

Insert this snippet to your terminal...

```
mvn archetype:generate -DarchetypeGroupId=io.activej -DarchetypeArtifactId=archetype-http -DarchetypeVersion=4.0-beta1
```

... and open the project in your favourite IDE. Then, build the application and run it. Open your browser
on [localhost:8080](http://localhost:8080)
to see the "Hello World" message.

#### Fully-featured embedded web application server with Dependency Injection:

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

Some technical details regarding the above example:

- *Features a JAR file size of only 1.4 MB. In comparison, a minimal Spring web app size is approximately 17 MB*.
- *The cold start time is 0.65 sec.*
- *The [ActiveJ Inject](https://inject.activej.io) DI library which is used, is 5.5 times faster than Guice and hundreds
  of times faster than Spring.*

To learn more about ActiveJ, please visit https://activej.io or follow our
5-minute [getting-started guide](https://activej.io/tutorials/getting-started).

Examples for usage of the ActiveJ platform and all the ActiveJ libraries can be found
in [`examples`](https://github.com/activej/activej/tree/master/examples) module.
