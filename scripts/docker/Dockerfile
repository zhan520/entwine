FROM connormanning/pdal:grid-maintenance
MAINTAINER Connor Manning <connor@hobu.co>

ENV CC gcc
ENV CXX g++

ADD http://www.timeapi.org/utc/now /tmp/bust-cache
RUN git clone https://github.com/connormanning/entwine.git && \
    cd entwine && \
    git checkout grid-maintenance && \
    mkdir build && \
    cd build && \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    make -j4 && \
    make install

