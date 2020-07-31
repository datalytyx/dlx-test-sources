#!/usr/bin/env bash

wget https://download.oracle.com/otn_software/linux/instantclient/19800/oracle-instantclient19.8-basiclite-19.8.0.0.0-1.x86_64.rpm -O oracle-instantclient.rpm
sudo alien -i ./oracle-instantclient.rpm --scripts && rm oracle-instantclient.rpm