# Use a base image that includes Spark and Python
FROM bde2020/spark-base:3.1.1-hadoop3.2

# Copy your code files into the Docker image
COPY . /app

# Set the working directory
WORKDIR /app

# Install pyspark
RUN pip3 install -r requirements.txt

# Run your Python script
CMD ["python3", "prueba_Ripley.py", "testing.py"]
