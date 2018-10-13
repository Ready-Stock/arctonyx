# Use an official Python runtime as a parent image
FROM golang:1.11

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN go get -d github.com/Ready-Stock/raft-badger;

# Make port 80 available to the world outside this container
EXPOSE 6500 6501

# Define environment variable
ENV NAME World

# Run app.py when the container launches
CMD ["radger"]