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