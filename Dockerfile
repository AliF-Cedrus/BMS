# start by pulling the python image
FROM python:3.10.6

COPY ./requirements.txt /app/requirements.txt
WORKDIR /app

RUN pip install -r requirements.txt
COPY . /app
COPY xpdf-tools-linux-4.04/bin64/pdftotext /usr/local/bin/
EXPOSE 5000
ENTRYPOINT [ "python" ]
CMD ["app.py" ]