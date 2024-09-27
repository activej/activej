FROM openjdk:24-slim
WORKDIR /app
COPY target/${artifactId}.jar ./
ENTRYPOINT java $SYS_PROPS -jar ${artifactId}.jar

# Note :
# - Package jar file with `mvn clean package`
# - Build image with `docker build -t <your_image_name> -f src/docker/Dockerfile .`
# - Run container with `docker run <your_image_name>`