FROM python:2

MAINTAINER Jon Flaishans <www.flaishans.com>

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsasl2-dev

RUN cd tmp &&  wget https://github.com/mongodb/mongo-c-driver/releases/download/1.4.0/mongo-c-driver-1.4.0.tar.gz
RUN cd tmp &&  tar xzf mongo-c-driver-1.4.0.tar.gz
RUN cd tmp &&  cd mongo-c-driver-1.4.0 && \
    ./configure --enable-ssl --enable-sas && \
    make && \
    make install && \
    ldconfig
#RUN cd tmp && hg clone https://bitbucket.org/djcbeach/monary && cd monary && python setup.py install

COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -qr /tmp/requirements.txt

COPY . /src/
WORKDIR /src
EXPOSE 8787
CMD ["python", "tornado_main.py"]