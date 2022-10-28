FROM apache/airflow:2.4.2
COPY setup.cfg setup.py pyproject.toml requirements.txt .
RUN pip install --no-cache-dir -e .