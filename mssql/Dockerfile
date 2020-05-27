FROM mcr.microsoft.com/mssql/server:2017-latest-ubuntu

ARG PASSWORD
ARG SOURCE

ENV SA_PASSWORD $PASSWORD
ENV ACCEPT_EULA Y
ENV MSSQL_PID Developer

RUN mkdir /database
ADD $SOURCE /database

ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ADD run-initialization.sh /run-initialization.sh
RUN chmod +x /run-initialization.sh

CMD /entrypoint.sh