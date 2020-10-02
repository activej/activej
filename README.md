[![Maven Central](https://img.shields.io/maven-central/v/io.activej/activej)](https://mvnrepository.com/artifact/io.activej)
[![GitHub](https://img.shields.io/github/license/activej/activej)](https://github.com/activej/activej/blob/master/LICENSE)

## Introduction

[ActiveJ](https://activej.io) is a fully-featured alternative Java platform built from the ground up as a replacement of Spring, 
Spark, Quarkus, Micronauts, and other solutions. It is minimalistic, boilerplate-free, and incomparably faster, which is proven by benchmarks.
ActiveJ has very few third-party dependencies, yet features a full stack of technologies with an efficient async programming model and a powerful 
DI library [ActiveInject](https://inject.activej.io)

## Quick start

Insert this snippet to your terminal...

```
mvn archetype:generate -DarchetypeGroupId=io.activej -DarchetypeArtifactId=archetype-http -DarchetypeVersion=2.2
```

... and open the project in your favourite IDE. Then, build the application and run it. Open your browser on [localhost:8080](http://localhost:8080) 
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
- *The [ActiveInject](https://inject.activej.io) DI library which is used, is 5.5 times faster than Guice and hundreds of times faster than Spring.*

To learn more about ActiveJ, please visit https://activej.io or follow our 5-minute [getting-started 
guide](https://activej.io/tutorials/getting-started). 

## Repository Structure
This repository contains the [ActiveJ](https://activej.io) platform components along with helper ActiveJ libraries:
* [ActiveInject](https://inject.activej.io) - `core-inject` module;
* [ActiveSerializer](https://serializer.activej.io) - `core-serializer` module;
* [ActiveCodegen](https://codegen.activej.io) - `core-codegen` module;
* [ActiveSpecializer](https://specializer.activej.io) - `core-specializer` module;
* [ActiveRPC](https://rpc.activej.io) - `cloud-rpc` module;
* [ActiveFS](https://fs.activej.io) - `cloud-fs` module.

Examples for usage of the ActiveJ platform and all the ActiveJ libraries can be found in [`examples`](https://github.com/activej/activej/tree/master/examples) module.
