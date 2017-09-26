FROM python:3-alpine
LABEL maintainer="butlerx <cian@coderdojo.com>"
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . /usr/src/app
CMD [ "python", "./etl/main.py" ]
