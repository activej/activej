## A tutorial for building and executing ActiveJ HTTP server as GraalVM native image

This tutorial represents a simple HTTP server that returns a "Hello, world!" response.
Native image maven [plugin](https://mvnrepository.com/artifact/org.graalvm.buildtools/native-maven-plugin) is used in this tutorial 
to simplify build process. Although, a `native-image` tool can be used directly if needed.

A tutorial also includes a reflection configuration file `reflectionconfig.json` to allow processing of ActiveJ Inject annotations. 

### Prerequisites
You need to have GraalVM and native-image tool installed as described [here](https://graalvm.github.io/native-build-tools/0.9.7.1/graalvm-setup.html).

### Building a native image
To build a native image execute `mvn -Pnative package` command from within 'native-image' module directory.

### Running a native image
To run a native image execute `./target/hello-world-server` command from within 'native-image' module directory.
