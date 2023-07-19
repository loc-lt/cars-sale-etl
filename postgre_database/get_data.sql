-- 0. use dgscli to crawl data from dgscli of canhtran to /data folder
/*
# Generate 1000000 products
$ dgscli generate --data product --num 1000000

# Generate 1000000 customers
$ dgscli generate --data user --num 1000000

# Generate 2000000 orders of 300000 customers and 500000 products
$ dgscli generate --data order --num 2000000 --no_userid 300000 --no_productid 500000
*/

-- 1. copy data from Window 10 to Ubuntu after share all data hast just shared with VM
/*
cp -a /mnt/hgfs/data/. /home/loclt/Desktop/carsales_etl_project/data/
*/