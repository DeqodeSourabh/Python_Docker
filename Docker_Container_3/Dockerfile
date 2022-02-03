#Deriving the latest base image
FROM python:latest


#Labels as key value pair
LABEL Maintainer="sourabh.a3"


# Any working directory can be chosen as per choice like '/' or '/home' etc
# i have chosen /usr/app/src
WORKDIR /home/Desktop/Projects/python_Docker/Docker_Container_3

#to COPY the remote file at working directory in container
COPY find_holders.py ./
# Now the structure looks like this '/usr/app/src/test.py'


#CMD instruction should be used to run the software
#contained by your image, along with any arguments.

CMD [ "python3", "./find_holders.py"]