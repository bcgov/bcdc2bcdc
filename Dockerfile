FROM python:3.6-alpine
ENV LANG C.UTF-8
RUN pip install bcdc2bcdc
ENV PYTHONPATH='/usr/local/lib/python3.6:/usr/local/lib/python3.6/site-packages:/usr/local/bin'
ENTRYPOINT ["python3", "/usr/local/bin/runBCDC2BCDC.py"]
