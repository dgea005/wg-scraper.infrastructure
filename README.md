# wg scraper

scrape wg listings and put to s3 and rds
process from rds and user preferences the lasting listings

# useful links

https://gist.github.com/powerc9000/2652e6dbfbc9d428ce41531dfc0b75cb
https://chankongching.wordpress.com/2015/12/30/devops-using-aws-cloudformation-to-create-postgresql-database/
http://www.stojanveselinovski.com/blog/2016/01/12/simple-postgresql-rds-cloudformation-template/



# postgres resources

## connection

psql \
   --host=<DB instance endpoint> \
   --port=<port> \
   --username <master user name> \
   --password \
   --dbname=<database name> 

need to add security group rules for this to work


## testing kinesis 

with example record

aws kinesis put-record \
--stream-name stream name  \
--data '{"link": "http://last_fired","listing_id": "000000", "cost": 400, "size": 18, "stadt": "Kreuzberg", "free_from": "2017-08-29", "free_to": "2017-09-01", "stay_length": 30, "scrape_time": "2017-08-24 07:20:13", "flat_type": "studio"}' \
--partition-key shardId-000000000000 \
--region my region \
--profile my profile