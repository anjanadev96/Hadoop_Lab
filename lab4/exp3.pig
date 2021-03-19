--load files
ip_input_lines = LOAD '/data/ip_trace' USING PigStorage(' ') AS (time:chararray, id:long, srcip:chararray, bracket:chararray, destip:chararray, protocol:chararray, dep:chararray);
raw_block_input_lines = LOAD '/data/raw_block' USING PigStorage(' ') AS (id:long, status:chararray);

--keep only blocked ones
blocked_lines = FILTER raw_block_input_lines BY status == 'Blocked';

--join the two relations
joined = JOIN ip_input_lines BY id, blocked_lines BY id;

--generate the columns we care to keep
firewall = FOREACH joined GENERATE time, ip_input_lines::id AS id, SUBSTRING(srcip,0, LAST_INDEX_OF(srcip,'.')) AS srcip, SUBSTRING(destip, 0, LAST_INDEX_OF(destip,'.')) AS destip, status;

--output
STORE firewall INTO '/lab4/exp3/firewall' USING PigStorage(' ');

--group and count
groups = GROUP firewall BY srcip;
counts = FOREACH groups GENERATE group, COUNT(firewall) AS count;

--sort and store
ordered = ORDER counts BY count DESC;
STORE ordered INTO '/lab4/exp3/output' USING PigStorage('\t');
