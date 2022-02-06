#Deriving the latest base image
FROM python:latest

RUN pip install web3
RUN pip install ray

#Labels as key value pair
LABEL Maintainer="sourabh.a1"


# Any working directory can be chosen as per choice like '/' or '/home' etc
# i have chosen /usr/app/src
WORKDIR /home/Desktop/Projects/python_Docker/Docker_Container_1

#to COPY the remote file at working directory in container
COPY find_blocks.py ./
# Now the structure looks like this '/usr/app/src/test.py'


#CMD instruction should be used to run the software
#contained by your image, along with any arguments.

CMD [ "python3", "./find_blocks.py"]