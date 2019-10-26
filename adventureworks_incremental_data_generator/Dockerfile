FROM python:3.7.4-buster
RUN apt-get update && apt-get install -y --no-install-recommends \
      uuid-dev cron logrotate \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg-agent \
      vim \
      rsyslog \
      software-properties-common 

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn

CMD ["python3 -u adventureworks_incremental_data_generator.py  --host 172.17.0.1 --port 4000 --database adventureworks --username datalytyx --password horsewelltree --sleep 0"]
