FROM openjdk:24-slim
WORKDIR /app
COPY target/${artifactId}.jar ./
EXPOSE 8080
ENV SYS_PROPS='-Dconfig.http.listenAddresses=0.0.0.0:8080'
ENTRYPOINT java $SYS_PROPS -jar ${artifactId}.jar

# Note :
# - Package jar file with `mvn clean package`
# - Build image with `docker build -t <your_image_name> -f src/docker/Dockerfile .`
# - Run container with `docker run -p 8080:8080 <your_image_name>`
