--cria as tabelas
CREATE TABLE "ACESSO_FULL" 
   ("SQL_HASH_VALUE" NUMBER, 
	"SQL_ID" VARCHAR2(13 BYTE), 
	"SQL_FULLTEXT" CLOB
   );

CREATE UNIQUE INDEX "IDX_ACESSO_FULL" ON "OLIVEIRAGILPERTO"."ACESSO_FULL" ("SQL_HASH_VALUE");

  CREATE TABLE "OCORRENCIA_FULL" 
   ("OCORRENCIA" DATE, 
	"SCHEMANAME" VARCHAR2(30 BYTE), 
	"OSUSER" VARCHAR2(30 BYTE), 
	"OBJECT_NAME" VARCHAR2(30 BYTE), 
	"MODULE" VARCHAR2(64 BYTE), 
	"ACTION" VARCHAR2(64 BYTE), 
	"SQL_HASH_VALUE" NUMBER, 
	"SQL_ID" VARCHAR2(13 BYTE), 
	"QUANTIDADE" NUMBER
   );


alter table ocorrencia_full add quantidade number;

--procedure que registra o acesso full
create or replace procedure pr_acesso_full
is
cursor cr_ocorrencia is 
    select  distinct y.sql_hash_value, y.sql_id
    from v$session y, v$sql x, v$sql_plan z
    where y.sql_hash_value = x.hash_value and y.sql_hash_value = z.hash_value
    and z.operation = 'TABLE ACCESS' and z.options = 'FULL'
    and osuser not in ('SYS', 'SYSTEM')
    and schemaname not in ('SYS', 'SYSTEM')
    and object_owner not in ('SYS', 'SYSTEM')
    and object_name not in ('USER$')
    and status = 'ACTIVE';
comando clob;    
total number;
begin 
    for linha in cr_ocorrencia loop
        select count(*) into total from acesso_full
        where sql_hash_value = linha.sql_hash_value;
        if total = 0
        then insert into acesso_full 
            select  linha.sql_hash_value, linha.sql_id, SQL_FULLTEXT
            from v$sql 
            where hash_value = linha.sql_hash_value;
        end if;
        
        select count(*) into total 
        from ocorrencia_full
        where to_char(ocorrencia, 'DDMMYYYY') = to_char(sysdate, 'DDMMYYYY')
        and (schemaname, osuser, sql_hash_value) in (
        select y.schemaname, y.osuser, y.sql_hash_value
        from v$session y, v$sql x, v$sql_plan z
        where y.sql_hash_value = x.hash_value and y.sql_hash_value = z.hash_value
        and z.operation = 'TABLE ACCESS' and z.options = 'FULL'
        and osuser not in ('SYS', 'SYSTEM')
        and schemaname not in ('SYS', 'SYSTEM')
        and object_owner not in ('SYS', 'SYSTEM')
        and object_name not in ('USER$')
        and status = 'ACTIVE'
        and y.sql_hash_value = linha.sql_hash_value
        );

        if total = 0
        then
            insert into ocorrencia_full select distinct sysdate, y.schemaname, y.osuser, object_name, y.module, y.action, y.sql_hash_value, y.sql_id, 1
                                        from 
                                            (select  listagg(object_name, ',') within group (order by object_name) object_name, sql_hash_value
                                            from (select distinct z.object_name, y.sql_hash_value
                                                    from v$session y, v$sql x, v$sql_plan z
                                                    where y.sql_hash_value = x.hash_value and y.sql_hash_value = z.hash_value
                                                    and z.operation = 'TABLE ACCESS' and z.options = 'FULL'
                                                    and osuser not in ('SYS', 'SYSTEM')
                                                    and schemaname not in ('SYS', 'SYSTEM')
                                                    and object_owner not in ('SYS', 'SYSTEM')
                                                    and object_name not in ('USER$')
                                                    and status = 'ACTIVE')
                                            group by sql_hash_value    
                                            ) x,
                                            (   select y.schemaname, y.osuser, y.module, y.action, y.sql_hash_value, y.sql_id
                                                from v$session y, v$sql x, v$sql_plan z
                                                where y.sql_hash_value = x.hash_value and y.sql_hash_value = z.hash_value
                                                and z.operation = 'TABLE ACCESS' and z.options = 'FULL'
                                                and osuser not in ('SYS', 'SYSTEM')
                                                and schemaname not in ('SYS', 'SYSTEM')
                                                and object_owner not in ('SYS', 'SYSTEM')
                                                and object_name not in ('USER$')
                                                and status = 'ACTIVE'
                                            ) y
                                        where x.sql_hash_value = y.sql_hash_value
                                        and x.sql_hash_value = linha.sql_hash_value;
        else
            update ocorrencia_full set quantidade = quantidade +1
            where to_char(ocorrencia, 'DDMMYYYY') = to_char(sysdate, 'DDMMYYYY')
            and (schemaname, osuser, sql_hash_value) in (
            select y.schemaname, y.osuser, y.sql_hash_value
            from v$session y, v$sql x, v$sql_plan z
            where y.sql_hash_value = x.hash_value and y.sql_hash_value = z.hash_value
            and z.operation = 'TABLE ACCESS' and z.options = 'FULL'
            and osuser not in ('SYS', 'SYSTEM')
            and schemaname not in ('SYS', 'SYSTEM')
            and object_owner not in ('SYS', 'SYSTEM')
            and object_name not in ('USER$')
            and status = 'ACTIVE');
        end if;
    end loop;
end;
/

--Para criar o scheduler
BEGIN
    DBMS_SCHEDULER.CREATE_SCHEDULE (

        repeat_interval  => 'FREQ=MINUTELY;BYDAY=MON,TUE,WED,THU,FRI',
     
        start_date => TO_TIMESTAMP_TZ('2023-09-05 11:20:00.000000000 AMERICA/SAO_PAULO','YYYY-MM-DD HH24:MI:SS.FF TZR'),
        schedule_name  => '"FULLTABLE_REGISTER"');

END;


-- Para criar o job
begin
dbms_scheduler.create_job(
job_name => 'registro_acessofull'
,job_type => 'PLSQL_BLOCK'
,job_action => 'begin if to_char(sysdate, ''HH24'') between ''07'' and ''19'' then  pr_acesso_full(); end if; end;'
,schedule_name =>'FULLTABLE_REGISTER'
,enabled => TRUE);
end;
/

--relatorio mensal
set serveroutput on;
clear screen;
declare
cursor cr_usuario is select distinct osuser, object_name, module, action, sql_hash_value, sql_id, quantidade, schemaname, ocorrencia from ocorrencia_full
                        where to_char(ocorrencia, 'YYYYMM') = to_char(sysdate, 'YYYYMM')
                        order by osuser;
comando clob;
usuario varchar2(3000):='x';
total number:=0;
begin

for u in cr_usuario loop
    if usuario != u.osuser
    then 
        dbms_output.put_line('*************************************************');
        dbms_output.put_line('Usuario: '||u.osuser);
        usuario := u.osuser;
    end if;
    dbms_output.put_line('SQL_HASH: '||u.sql_hash_value);
    dbms_output.put_line('SQL_ID: '||u.sql_id);
    dbms_output.put_line('Schemma: '||u.schemaname);
    dbms_output.put_line('Module: '||u.module);
    dbms_output.put_line('Action: '||u.action);
    dbms_output.put_line('Tabelas: '||u.object_name);
    dbms_output.put_line('Hora do 1o registro: '||u.ocorrencia);
    select sum(quantidade) into total
    from ocorrencia_full
    where osuser = u.osuser
    and sql_hash_value = u.sql_hash_value
    and to_char(ocorrencia, 'YYYYMM') = to_char(sysdate, 'YYYYMM');
    dbms_output.put_line('Núm de Ocorrências: '||total);
    dbms_output.put_line('Comando: ');
    select sql_fulltext into comando from acesso_full
    where sql_hash_value = u.sql_hash_value;
    dbms_output.put_line(comando);
    dbms_output.put_line('');
end loop;
end;
/


--relatorio dia -1 
set serveroutput on;
clear screen;
declare
cursor cr_usuario  is select distinct osuser, object_name, module, action, sql_hash_value, sql_id, quantidade, schemaname, ocorrencia from ocorrencia_full
                        where to_char(ocorrencia, 'YYYYMMDD') = to_char(sysdate-1, 'YYYYMMDD')
                        order by osuser;
inicio date;
fim date;
comando clob;
usuario varchar2(3000):='x';
begin

for u in cr_usuario loop
    if usuario != u.osuser
    then 
        dbms_output.put_line('*************************************************');
        dbms_output.put_line('Usuario: '||u.osuser);
        usuario := u.osuser;
    end if;
    dbms_output.put_line('SQL_HASH: '||u.sql_hash_value);
    dbms_output.put_line('SQL_ID: '||u.sql_id);
    dbms_output.put_line('Schemma: '||u.schemaname);
    dbms_output.put_line('Module: '||u.module);
    dbms_output.put_line('Action: '||u.action);
    dbms_output.put_line('Tabelas: '||u.object_name);
    dbms_output.put_line('Hora do 1o acesso: '||u.ocorrencia);
    dbms_output.put_line('Núm de Ocorrências: '||u.quantidade);
    dbms_output.put_line('Comando: ');
    select sql_fulltext into comando from acesso_full
    where sql_hash_value = u.sql_hash_value;
    dbms_output.put_line(comando);
    dbms_output.put_line('');
end loop;
end;
/


--relatorio hoje
set serveroutput on;
clear screen;
declare
cursor cr_usuario  is select distinct osuser, object_name, module, action, sql_hash_value, sql_id, quantidade, schemaname, ocorrencia from ocorrencia_full
                        where to_char(ocorrencia, 'YYYYMMDD') = to_char(sysdate, 'YYYYMMDD')
                        order by osuser;
inicio date;
fim date;
comando clob;
usuario varchar2(3000):='x';
begin

for u in cr_usuario loop
    if usuario != u.osuser
    then 
        dbms_output.put_line('*************************************************');
        dbms_output.put_line('Usuario: '||u.osuser);
        usuario := u.osuser;
    end if;
    dbms_output.put_line('SQL_HASH: '||u.sql_hash_value);
    dbms_output.put_line('SQL_ID: '||u.sql_id);
    dbms_output.put_line('Schemma: '||u.schemaname);
    dbms_output.put_line('Module: '||u.module);
    dbms_output.put_line('Action: '||u.action);
    dbms_output.put_line('Tabelas: '||u.object_name);
    dbms_output.put_line('Hora do 1o acesso: '||u.ocorrencia);
    dbms_output.put_line('Núm de Ocorrências: '||u.quantidade);
    dbms_output.put_line('Comando: ');
    select sql_fulltext into comando from acesso_full
    where sql_hash_value = u.sql_hash_value;
    dbms_output.put_line(comando);
    dbms_output.put_line('');
end loop;
end;
/


