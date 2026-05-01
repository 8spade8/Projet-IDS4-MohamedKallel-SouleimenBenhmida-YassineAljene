FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

COPY requirements.txt .
COPY wheelhouse ./wheelhouse
RUN pip install --no-cache-dir --no-index --find-links=/app/wheelhouse -r requirements.txt

COPY data.py .
COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
