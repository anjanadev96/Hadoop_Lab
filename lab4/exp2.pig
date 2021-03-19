--load the data
inputlines = LOAD '/data/network_trace' USING PigStorage(' ') AS (time:chararray, IP:chararray, srcip:chararray, arrow:chararray, destip: chararray, protocol:chararray, dep:chararray);

--filter the data to only keep TCP
filteredlines = FILTER inputlines BY protocol=='tcp';

--remove part e from ip address of the from ip
ippairs = FOREACH filteredlines GENERATE SUBSTRING(srcip,0, LAST_INDEX_OF(srcip,'.')) AS srcip, SUBSTRING(destip, 0, LAST_INDEX_OF(destip,'.')) AS destip;

--remove duplicates
distinctippairs = DISTINCT ippairs;

--group by src ip and count dest ips
groupedips = GROUP distinctippairs BY srcip;
total = FOREACH groupedips GENERATE group, COUNT(distinctippairs) AS count;

--sort in order of number of dests
sortedips = ORDER total BY count DESC;
top_10 = LIMIT sortedips 10;
STORE top_10 INTO '/lab4/exp2/output/' USING PigStorage('\t');







