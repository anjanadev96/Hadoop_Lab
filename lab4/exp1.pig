--load file
input_lines = LOAD 'gaz_tracts_national.txt' USING PigStorage('\t') AS (usps:chararray, geoid:chararray, pop10:chararray,hu10:chararray, aland:long, awater:int, alandsqmi:int,awatersqmi:int, intptlat : int, intptlong : int);

--group by states
states = GROUP input_lines by usps;
total = FOREACH states GENERATE group, SUM(input_lines.aland) AS sum;

--order by decreasing and select top 10
ordered_aland = ORDER total BY sum DESC;
top_10 = LIMIT ordered_aland 10;
STORE top_10 INTO 'lab4_exp1_output' USING PigStorage('\t');
 
