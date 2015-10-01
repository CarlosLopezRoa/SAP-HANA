CREATE PROCEDURE "RFC_USER"."UNLOAD_MEMORY" /*( out out_t varchar(600) )*/
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER as 
BEGIN
DECLARE table_name nvarchar(513) ARRAY; 
declare n_v int;
declare t_name nvarchar(513);
declare i int;

tablas= select schema_name||'.'||table_name as name 
from SYS.M_CS_TABLES 
where SCHEMA_NAME IN ('T1', 'T2', 'RFC_USER')
and memory_size_in_total>0;

select count(1) into n_v from :tablas;
table_name:=ARRAY_AGG(:tablas.name);

For i in 1 ..:n_v 
	DO
	t_name:= 'unload '||:table_name[:i];
	EXEC :t_name;	
end for;	

--out_t:= :t_name;
end; 