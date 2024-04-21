FROM mageai/mageai:latest

# ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}
ARG USER_CODE_PATH=/home/src

# Note: this overwrites the requirements.txt file in your new project on first run. 
# You can delete this line for the second run :) 
COPY requirements.txt ${USER_CODE_PATH}/requirements.txt 
COPY gcp.json ${USER_CODE_PATH}/gcp.json
COPY . ${USER_CODE_PATH}

RUN pip install -r ${USER_CODE_PATH}/requirements.txt
RUN su -
RUN apt-get update -y
RUN apt-get install -y fonts-liberation
RUN apt-get install -y libasound2
RUN apt-get install -y libatk-bridge2.0-0
RUN apt-get install -y libatk1.0-0
RUN apt-get install -y libatspi2.0-0
RUN apt-get install -y libcups2
RUN apt-get install -y libdbus-1-3
RUN apt-get install -y libdrm2
RUN apt-get install -y libgbm1
RUN apt-get install -y libgtk-3-0
RUN apt-get install -y libnspr4
RUN apt-get install -y libnss3
RUN apt-get install -y libu2f-udev
RUN apt-get install -y libvulkan1
RUN wget http://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_114.0.5735.90-1_amd64.deb
RUN dpkg -i google-chrome-stable_114.0.5735.90-1_amd64.deb
RUN wget -N https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip -P ~/
RUN unzip -o chromedriver_linux64.zip -d .
RUN rm chromedriver_linux64.zip
RUN rm google-chrome-stable_114.0.5735.90-1_amd64.deb
RUN mv -f chromedriver /usr/local/bin/chromedriver

# EXPOSE 6789
CMD ["mage", "start", "oz-grocery-price-analysis"]