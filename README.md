# public-score-data

This repo contains the following files:

score_downloader.py, which has the functionality to curl live scores for tennis and soccer from sofascore.com and write them to my MySQL database.
sql.py, which provides connectivity to the database
log_notify.py, which logs all output printed within score_downloader.py to logs.log along with a timestamp. log_notify.py also has the functionality to send a notifying slack message if score_downloader.py has any issues. 
