# To build image for this dockerfile use this command:
#   docker build -t pyapp -f environment/Dockerfile .
#
# To run without compose but with shell terminal use this command:
#   docker run -p 127.0.0.1:5000:5000 -v $PWD/rankservice/src:/usr/local/rankservice/src -it pyapp sh
#
# To run without compose and without shell terminal use this command:
#   docker run -p 127.0.0.1:5000:5000 -v $PWD/rankservice/src:/usr/local/rankservice/src pyapp
#
#--------- Generic stuff all our Dockerfiles should start with so we get caching ------------
FROM python:3.6-alpine

LABEL "br.inpe.dpi"="INPE/DPI-TerraBrasilis"
LABEL br.inpe.dpi.terrabrasilis="microservice"
LABEL version="0.1"
LABEL author="Andre Carvalho"
LABEL author.email="carvalho@dpi.inpe.br"
LABEL description="This microservice provides a sort \
feature to rank cells to DETER cerrado project."

ENV PYTHONUNBUFFERED 1
#-------------Application Specific Stuff ----------------------------------------------------

RUN apk update \
  && apk add \
    build-base \
    postgresql \
    postgresql-dev \
    libpq

ENV INSTALL_PATH /usr/local/rankservice

RUN mkdir -p $INSTALL_PATH/config
RUN mkdir -p $INSTALL_PATH/src

ADD rankservice/config/requirements.txt $INSTALL_PATH/config/requirements.txt

# Install Python requirements
RUN pip install -r $INSTALL_PATH/config/requirements.txt

ENV APP_EXEC=$INSTALL_PATH/src/

VOLUME ["${INSTALL_PATH}/src/"]

EXPOSE 5000

WORKDIR $APP_EXEC
#CMD ["python", "app.py"]

# Without define workdir
# ENV APP_EXEC=$INSTALL_PATH/src/app.py
# CMD ["sh", "-c", "python ${APP_EXEC}"]