FROM python:3-alpine
LABEL maintainer="butlerx <cian@coderdojo.com>"
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apk add --update build-base postgresql-dev postgresql && \
    pip install --no-cache-dir -r requirements.txt && \
    mkdir /db && \
    apk del build-base
COPY . /usr/src/app
ENTRYPOINT [ "python", "etl" ]
CMD ["--db-path", "/db/"]
