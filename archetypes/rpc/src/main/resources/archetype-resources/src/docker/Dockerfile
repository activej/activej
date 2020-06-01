FROM openjdk:8-jre-alpine
WORKDIR /app
COPY target/${artifactId}.jar ./
EXPOSE 8080
ENTRYPOINT java $SYS_PROPS -jar ${artifactId}.jar

# Note : run it with `docker build -t <your_image_name> -f src/docker/Dockerfile .`
