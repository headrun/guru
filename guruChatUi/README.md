# Wall
wall.buzzinga.com

## Global Dependencies
1. Node
2. Python 2.7
3. pip
4. nginx
5. mysql 5.7
6. bower `npm install -g bower`
7. grunt `npm install -g grunt-cli`
8. python-virtualenv
9. git
10. libmysqlclient-dev, libjpeg8-dev
11. sudo apt-get install cron
12. Install lockrun (http://www.unixwiz.net/tools/lockrun.html)

## Setup MOP
1. `cd backend;`
2. `virtualenv venv;`
3. `source venv/bin/activate;`
4. `pip install -r requirements.pip`
5. Do `git update-index --assume-unchanged backend/local_settings.py` (ignore local settings)
6. cd ../ui;
7. `npm install`
8. `bower install`
9. `grunt init`
10. `cd ..`
11. `cp .vimrc ~/`

### Scripts scheduling
1. put the following in crontab with correct paths `* * * * * /usr/local/bin/lockrun --lockfile /tmp/pull_tweets.lock -- bash -c "export PATH=$PATH:/usr/local/bin/; cd <path to >/PR/backend; source venv/bin/activate; python manage.py pull_tweets" 1>> /dev/null 2>> /dev/null`

### Django server - Development
1. Make sure virtualenv is on
2. Configure database name in `backend/local_settings.py`
3. Do django migrations
4. Do runserver

### Django server - Production
1. put the following in crontab with desired paths, sock and lock files `* * * * * /usr/local/bin/lockrun --lockfile=/tmp/<lock file>.lock -- <path to>/PR/backend/venv/bin/uwsgi --close-on-exec -s /tmp/<sock file>.sock --chdir <path to>/PR/backend/ -C666 -p 16 --plugin python -H <path to>/PR/backend/venv/ --file <path to>/PR/backend/backend/wsgi.py 2>> /tmp/<error log file>.log 1>> /dev/null`

### nginx conf
1. copy pr.conf to /etc/nginx/sites-enabled/ and change the paths
2. give your desired `server_name`, and give the same value for `proxy_set_header X-Forwarded-Host`

### nginx conf - Development
1. in nginx conf, uncomment the line below "development" comment and make sure the `poxy_pass` is pointed to the port in which django server is running
2. `service nginx reload`

### nginx conf - Production
1. in nginx conf, uncommnet the code below "production" comment and make sure the right sock file ( sock file given in Django Server Production cron job) is given for `uwsgi_pass` setting
2. `service nginx reload`
