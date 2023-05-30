[![Maven Central](https://img.shields.io/maven-central/v/io.activej/activej)](https://mvnrepository.com/artifact/io.activej)
[![GitHub](https://img.shields.io/github/license/activej/activej)](https://github.com/activej/activej/blob/master/LICENSE)

## Introduction

[ActiveJ](https://activej.io) is a modern Java platform built from the ground up.
It is designed to be self-sufficient (no third-party dependencies), simple, lightweight and provides competitive performance.
ActiveJ consists of a range of orthogonal libraries, from dependency injection and high-performance
asynchronous I/O (inspired by Node.js), to application servers and big data solutions. 

These libraries have as few dependencies as possible with respect to each other and can be used together or separately. 
ActiveJ in not yet another framework that forces the user to use it on an all-or-nothing basis, but instead it gives 
the user as much freedom as possible in terms of choosing library components for particular tasks.

## ActiveJ components

ActiveJ consists of several modules, which can be logically grouped into the following categories :

* **Async.io** - High-performance asynchronous IO with the efficient event loop, NIO, promises, streaming, and CSP.
  Alternative to Netty, RxJava, Akka, and others. ([Promise](https://activej.io/async-io/promise),
  [Eventloop](https://activej.io/async-io/eventloop), [Net](https://activej.io/async-io/net),
  [CSP](https://activej.io/async-io/csp), [Datastream](https://activej.io/async-io/datastream))

  ```java
  // Basic eventloop usage
  public static void main(String[] args) {
      Eventloop eventloop = Eventloop.create();
  
      eventloop.post(() -> System.out.println("Hello, world!"));
  
      eventloop.run();
  }
  ```

  ```java
  // Promise chaining
  public static void main(String[] args) {
      Eventloop eventloop = Eventloop.builder()
          .withCurrentThread()
          .build();
  
      Promises.delay(Duration.ofSeconds(1), "world")
          .map(string -> string.toUpperCase())
          .then(string -> Promises.delay(Duration.ofSeconds(3))
              .map($ -> "HELLO " + string))
          .whenResult(string -> System.out.println(string));
  
      eventloop.run();
  }
  ```

  ```java
  // CSP workflow example
  ChannelSuppliers.ofValues(1, 2, 3, 4, 5, 6)
      .filter(x -> x % 2 == 0)
      .map(x -> 2 * x)
      .streamTo(ChannelConsumers.ofConsumer(System.out::println));
  ```

  ```java
  // Datastream workflow example
  StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6)
      .transformWith(StreamTransformers.filter(x -> x % 2 == 0))
      .transformWith(StreamTransformers.mapper(x -> 2 * x))
      .streamTo(StreamConsumers.ofConsumer(System.out::println));
  ```

* **HTTP** - High-performance HTTP server and client with WebSocket support. It can be used as a simple web server or as an
  application server. Alternative to other conventional HTTP clients and servers. ([HTTP](https://activej.io/http))

  ```java
  // Server
  public static void main(String[] args) throws IOException {
      Eventloop eventloop = Eventloop.create();
  
      AsyncServlet servlet = request -> HttpResponse.ok200()
          .withPlainText("Hello world")
          .toPromise();
  
      HttpServer server = HttpServer.builder(eventloop, servlet)
          .withListenPort(8080)
          .build();
  
      server.listen();
  
      eventloop.run();
  }
  ```

  ```java
  // Client
  public static void main(String[] args) {
      Eventloop eventloop = Eventloop.create();
  
      HttpClient client = HttpClient.create(eventloop);
  
      HttpRequest request = HttpRequest.get("http://localhost:8080").build();
  
      client.request(request)
          .then(response -> response.loadBody())
          .map(body -> body.getString(StandardCharsets.UTF_8))
          .whenResult(bodyString -> System.out.println(bodyString));
  
      eventloop.run();
  }
  ```

* **ActiveJ Inject** - Lightweight library for dependency injection. Optimized for fast application start-up and
  performance at runtime. Supports annotation-based component wiring as well as reflection-free
  wiring. ([ActiveJ Inject](https://activej.io/inject))

  ```java
  // Manual binding
  public static void main(String[] args) {
      Module module = ModuleBuilder.create()
          .bind(int.class).toInstance(101)
          .bind(String.class).to(number -> "Hello #" + number, int.class)
          .build();
  
      Injector injector = Injector.of(module);
  
      String string = injector.getInstance(String.class);
  
      System.out.println(string); // "Hello #101"
  }
  ```
  
  ```java
  // Binding via annotations
  public static class MyModule extends AbstractModule {
      @Provides
      int number() {
          return 101;
      }
  
      @Provides
      String string(int number) {
          return "Hello #" + number;
      }
  }
  
  public static void main(String[] args) {
      Injector injector = Injector.of(new MyModule());
  
      String string = injector.getInstance(String.class);
  
      System.out.println(string); // "Hello #101"
  }
  ```

* **Boot** - Production-ready tools for running and monitoring an ActiveJ application. Concurrent control of services lifecycle 
  based on their dependencies. Various service monitoring utilities with JMX and Zabbix support. ([Launcher](https://activej.io/boot/launcher),
  [Service Graph](https://activej.io/boot/servicegraph), [JMX](https://github.com/activej/activej/tree/master/boot-jmx),
  [Triggers](https://github.com/activej/activej/tree/master/boot-triggers))

  ```java
  public class MyLauncher extends Launcher {
      @Inject
      String message;
  
      @Provides
      String message() {
          return "Hello, world!";
      }
  
      @Override
      protected void run() {
          System.out.println(message);
      }
  
      public static void main(String[] args) throws Exception {
          Launcher launcher = new MyLauncher();
          launcher.launch(args);
      }
  }
  ```

* **Bytecode manipulation**
    * **ActiveJ Codegen** - Dynamic bytecode generator for classes and methods on top of [ObjectWeb ASM](https://asm.ow2.io/)
      library. Abstracts the complexity of direct bytecode manipulation and allows you to create custom classes on the fly
      using Lisp-like AST expressions. ([ActiveJ Codegen](https://activej.io/codegen))

      ```java
      // Manually implemented method
      public class MyCounter implements Counter { 
          @Override
          public int countSum() {
              int sum = 0;
              for (int i = 0; i < 100; i++) {
                  sum += i;
              }
              return sum;
          }
      }
      ```
      
      ```java
      // The same method generated via ActiveJ Codegen 
      public static void main(String[] args) {
          DefiningClassLoader classLoader = DefiningClassLoader.create();
      
          Counter counter = ClassGenerator.builder(Counter.class)
              .withMethod("countSum",
                  let(value(0), sum ->
                      sequence(
                          iterate(
                              value(0),
                              value(100),
                              i -> set(sum, add(sum, i))),
                          sum
                      )))
              .build()
              .generateClassAndCreateInstance(classLoader);
      
          System.out.println(counter.countSum()); // 4950
      }
      ```

    * **ActiveJ Serializer** - [Fast](https://github.com/activej/jvm-serializers) and space-efficient serializers created with bytecode engineering.
      Introduces schema-free approach for best performance. ([ActiveJ Serializer](https://activej.io/serializer))
      
      ```java
      // A class to be serialized
      public class User {
          private final int id;
          private final String name;
      
          public User(@Deserialize("id") int id, @Deserialize("name") String name) {
              this.id = id;
              this.name = name;
          }
      
          @Serialize
          public int getId() {
              return id;
          }
      
          @Serialize
          public String getName() {
              return name;
          }
      }
      ```

      ```java
      // Serialization and deserialization
      public static void main(String[] args) {
          BinarySerializer<User> userSerializer = SerializerFactory.defaultInstance()
              .create(User.class);
      
          User john = new User(1, "John");
      
          byte[] buffer = new byte[100];
          userSerializer.encode(buffer, 0, john);
      
          User decoded = userSerializer.decode(buffer, 0);
      
          System.out.println(decoded.id);   // 1
          System.out.println(decoded.name); // John
      }
      ```

      ```java
      // Serialization of Java records
      @SerializeRecord
      public record User(int id, String name) {
      }
      ```
      
      ```java
      // StreamCodec usage example
      public static void main(String[] args) throws IOException {
          StreamCodec<User> userStreamCodec = StreamCodec.create(User::new,
              User::getId, StreamCodecs.ofVarInt(),
              User::getName, StreamCodecs.ofString()
          );
      
          List<User> users = List.of(
              new User(1, "John"),
              new User(2, "Sarah"),
              new User(3, "Ben")
          );
      
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try (StreamOutput streamOutput = StreamOutput.create(baos)) {
              for (User user : users) {
                  userStreamCodec.encode(streamOutput, user);
              }
          }
      
          ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
          try (StreamInput streamInput = StreamInput.create(bais)) {
              while (!streamInput.isEndOfStream()) {
                  User decoded = userStreamCodec.decode(streamInput);
                  System.out.println(decoded.getId() + " " + decoded.getName());
              }
          }
      }
      ```
      
    * **ActiveJ Specializer** - Innovative technology to improve class performance at runtime by automatically
      converting class instances into specialized static classes and class instance fields into baked-in static
      fields. Provides a wide variety of JVM optimizations for static classes that are impossible otherwise: dead code
      elimination, aggressive inlining of methods and static
      constants. ([ActiveJ Specializer](https://activej.io/specializer))

      ```java
      // Operators
      public record IdentityOperator() implements IntUnaryOperator {
          @Override
          public int applyAsInt(int operand) {
              return operand;
          }
      }
      
      public record ConstOperator(int value) implements IntUnaryOperator {
          @Override
          public int applyAsInt(int operand) {
              return value;
          }
      }
      
      public record SumOperator(IntUnaryOperator left, IntUnaryOperator right) implements IntUnaryOperator {
          @Override
          public int applyAsInt(int operand) {
              return left.applyAsInt(operand) + right.applyAsInt(operand);
          }
      }
      
      public record ProductOperator(IntUnaryOperator left, IntUnaryOperator right) implements IntUnaryOperator {
          @Override
          public int applyAsInt(int operand) {
              return left.applyAsInt(operand) * right.applyAsInt(operand);
          }
      }
      ```

      ```java
      // Expression specialization
      public static void main(String[] args) {
          // ((x + 10) * (-5)) + 33
          IntUnaryOperator expression = new SumOperator(
              new ProductOperator(
                  new ConstOperator(-5),
                  new SumOperator(
                      new ConstOperator(10),
                      new IdentityOperator()
                  )
              ),
              new ConstOperator(33)
          );
      
          Specializer specializer = Specializer.create();
          expression = specializer.specialize(expression);
      
          System.out.println(expression.applyAsInt(0));  // -17
      }
      ```

* **Cloud components**
    * **ActiveJ FS** - Asynchronous abstraction over the file system for building efficient, scalable local or remote file
      storages that support data redundancy, rebalancing, and resharding.
      ([ActiveJ FS](https://activej.io/fs))

      ```java
      public static void main(String[] args) throws IOException {
          Eventloop eventloop = Eventloop.builder()
              .withCurrentThread()
              .build();
      
          HttpClient httpClient = HttpClient.create(eventloop);
      
          ExecutorService executor = Executors.newCachedThreadPool();
      
          Path directory = Files.createTempDirectory("fs");
      
          FileSystem fileSystem = FileSystem.create(eventloop, executor, directory);
      
          String filename = "file.txt";
      
          fileSystem.start()
                  // Upload
                  .then(() -> fileSystem.upload(filename))
                  .then(consumer -> httpClient.request(HttpRequest.get("http://localhost:8080").build())
                      .then(response -> response.loadBody())
                      .then(body -> ChannelSuppliers.ofValue(body).streamTo(consumer)))
      
                  // Download
                  .then(() -> fileSystem.download(filename))
                  .then(supplier -> supplier.streamTo(ChannelConsumers.ofConsumer(byteBuf ->
                      System.out.println(byteBuf.asString(StandardCharsets.UTF_8)))))
      
                  // Cleanup
                  .whenComplete(executor::shutdown);
      
          eventloop.run();
      }
      ```

    * **ActiveJ RPC** - High-performance binary client-server protocol. Allows building distributed, sharded, and
      fault-tolerant microservice applications. ([ActiveJ RPC](https://activej.io/rpc))

      ```java
      // Server
      public static void main(String[] args) throws IOException {
          Eventloop eventloop = Eventloop.create();
      
          RpcServer server = RpcServer.builder(eventloop)
              .withMessageTypes(String.class)
              .withHandler(String.class, name -> Promise.of("Hello, " + name))
              .withListenPort(9000)
              .build();
      
          server.listen();
      
          eventloop.run();
      }
      ```

      ```java
      // Client
      public static void main(String[] args) {
          Eventloop eventloop = Eventloop.create();
      
          RpcClient client = RpcClient.builder(eventloop)
              .withStrategy(RpcStrategies.server(new InetSocketAddress(9000)))
              .withMessageTypes(String.class)
              .build();
      
          client.start()
              .then(() -> client.sendRequest("John"))
              .whenResult(response -> System.out.println(response)) // "Hello, John"
              .whenComplete(client::stop);
      
          eventloop.run();
      }
      ```

    * Various extra services:
      [ActiveJ CRDT](https://github.com/activej/activej/tree/master/extra/cloud-crdt),
      [Redis client](https://github.com/activej/activej/tree/master/extra/cloud-redis),
      [Memcache](https://github.com/activej/activej/tree/master/extra/cloud-memcache),
      [OLAP Cube](https://github.com/activej/activej/tree/master/extra/cloud-lsmt-cube),
      [Dataflow](https://github.com/activej/activej/tree/master/extra/cloud-dataflow)

## Quick start

Paste this snippet into your terminal...

```
mvn archetype:generate -DarchetypeGroupId=io.activej -DarchetypeArtifactId=archetype-http -DarchetypeVersion=5.5-rc3
```

... and open the project in your favorite IDE. Then build the application and run it. Open your browser
on [localhost:8080](http://localhost:8080)
to see the "Hello World" message.

#### Full-featured embedded web application server with Dependency Injection:

```java
public final class HttpHelloWorldExample extends HttpServerLauncher {
    @Provides
    AsyncServlet servlet() {
        return request -> HttpResponse.ok200()
            .withPlainText("Hello, World!")
            .toPromise();
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