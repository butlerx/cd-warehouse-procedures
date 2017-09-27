FROM python:3-alpine
LABEL maintainer="butlerx <cian@coderdojo.com>"
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apk add --update build-base postgresql-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del build-base
COPY . /usr/src/app
ENV AWS_ACCESS AWS_SECRET
CMD [ "python", "./etl/main.py" ]
