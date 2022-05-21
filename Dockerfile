FROM python:3-buster
RUN apt-get install -y curl wget bash
COPY requirements.txt run_pychord.py dev-requirements.txt README.md /app/
COPY pychord /app/pychord/
COPY test_pychord /app/test_pychord/
RUN pip install -r /app/dev-requirements.txt
CMD ["python", "/app/run_pychord.py", "run-node", "/app/local.db", "--bind-address", "0.0.0.0"]