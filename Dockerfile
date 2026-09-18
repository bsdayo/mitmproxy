FROM mitmproxy/mitmproxy:12.2.3
RUN pip install --no-cache-dir httpx==0.28.1 aiomqtt==2.5.1 websockets==15.0.1
WORKDIR /app
COPY main.py .
COPY addons ./addons
CMD ["mitmdump", "-s", "/app/main.py"]
