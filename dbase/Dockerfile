FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y \
        apache2 \
        libapache2-mod-wsgi-py3 \
        python3 \
        python3-pip \
        python3-venv && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN a2enmod wsgi
RUN mkdir -p /var/www/site/logs

COPY site.conf /etc/apache2/sites-available/000-default.conf

EXPOSE 80
WORKDIR /var/www/site
ENV pubscan_DB="/var/www/site/parser/pubscan.db"
ENV pubscan_DB_names="/var/www/site/parser/names.db"
ENV pubscan_log_filename="/var/www/site/dbase/logs/search.log"

RUN sed -i 's|/var/log/apache2|/var/www/site/dbase/logs|g' /etc/apache2/envvars

CMD bash -c '\
    # Create venv if missing \
    if [ ! -d /var/www/site/dbase/venv ]; then \
        python3 -m venv /var/www/site/dbase/venv; \
    fi; \
    # Upgrade pip \
    /var/www/site/dbase/venv/bin/pip install --upgrade pip; \
    # Install requirements if exists \
    if [ -f /var/www/site/dbase/requirements.txt ]; then \
        /var/www/site/dbase/venv/bin/pip install -r /var/www/site/dbase/requirements.txt; \
    fi; \
    # Install pubscan in editable mode if folder exists \
    if [ -d /var/www/site/pubscan ]; then \
        /var/www/site/dbase/venv/bin/pip install -e /var/www/site/pubscan; \
    fi; \
    echo "docker server apache+mod_wsgi is running"; \
    # Start Apache in foreground \
    apache2ctl -D FOREGROUND' \

