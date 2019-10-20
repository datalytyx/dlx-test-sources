FROM mcr.microsoft.com/mssql/server:2019-latest

RUN apt-get update && apt-get install -y unzip vim

RUN wget https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks-oltp-install-script.zip && unzip AdventureWorks-oltp-install-script.zip

ADD entrypoint.sh /

CMD /entrypoint.sh
