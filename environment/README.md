# Docker for rank cells
Define the environment to run the cells sort process.

## Run the docker

To build image for this dockerfile use this command:
´´´sh
docker build -t pyapp -f environment/Dockerfile .
´´´

To run without compose but with shell terminal use this command:
´´´sh
docker run -p 127.0.0.1:5000:5000 -v $PWD/rankservice/src:/usr/local/rankservice/src -it pyapp sh
´´´

To run without compose and without shell terminal use this command:
´´´sh
docker run -p 127.0.0.1:5000:5000 -v $PWD/rankservice/src:/usr/local/rankservice/src pyapp
´´´