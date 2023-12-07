FROM prefecthq/prefect:2-python3.11

COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN apt update && apt install wget && apt -y install osmium-tool

COPY src /opt/prefect/src
COPY cities.json /opt/prefect

CMD ["python", "src/main.py"]