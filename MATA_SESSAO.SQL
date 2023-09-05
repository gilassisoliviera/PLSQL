clear screen;
create or replace PROCEDURE MATA_SESSAO (thread_id varchar2)
is
processo_origem varchar2(20);
processo_destino varchar2(20);
terminal_origem varchar2(20);
processo varchar2(30);
comando varchar2(2000):='';
cur number;
ret number;
ordem number;
valor_sid varchar2(20);
valor_serial varchar2(20);
valor_schema varchar2(20);
valor_maquina varchar2(20);
valor_identificador varchar2(100);
job_no number;
inst_vitima number;
inst_local number;
linguagem varchar2(30);
begin
	select count(process) into processo_origem from vw_sessao where audsid = (select sys_context('USERENV','SESSIONID') from dual);
	if (processo_origem > 0)
      then begin
        select substr(process, 0,instr(process,':',1,1)-1) into processo_origem from vw_sessao where audsid = (select sys_context('USERENV','SESSIONID') from dual) and PROGRAM not like 'oracle@%';
        select terminal into terminal_origem from vw_sessao where audsid = (select sys_context('USERENV','SESSIONID') from dual) and PROGRAM not like 'oracle@%';
      end;
	end if;
	SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - PROCESSO: '||trim(trim(processo_origem)||':'||trim(thread_id))||' - INICIO: '||to_char(sysdate, 'YYYYMMDD HH24MISS'));
	select  count(process) into processo_destino  from vw_sessao where trim(upper(terminal)) = trim(upper(terminal_origem)) and trim(process) = trim(trim(processo_origem)||':'||trim(thread_id));
  SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - Qde de sessões de destino: = '||processo_destino);
	if (processo_destino > 0)
    then begin
        SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - Inicio das pesquisas das sessoes de destino.');
        processo:= trim(to_char(to_number(processo_origem)||':'||to_number(thread_id)));
        for x in (select sid valor_sid, schemaname valor_schema from vw_sessao where process like trim(processo) and terminal like trim(terminal_origem)) loop
          --SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - select distinct linguagem into linguagem from ctool.ctt_login where lgn_bd_usu = '||x.valor_schema);
          select distinct linguagem into linguagem from ctool.ctt_login where lgn_bd_usu = x.valor_schema;
          if linguagem = 'COBOL'
            then begin
              SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - USUARIO: '||x.valor_schema);
              SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - PROCESSO: '||trim(trim(processo_origem)||':'||trim(thread_id))||' - SID: '||x.valor_sid);
              select serial# into valor_serial  from vw_sessao where process like processo and terminal like terminal_origem and sid = x.valor_sid;
              select to_char(localtimestamp(2),'YYYYMMDD_HH24MISSFF')||';'||valor_schema into valor_identificador from dual;
              insert into registro_mata_sessao values (sysdate, 'USUARIO (vitima)= '||x.valor_schema, trim(trim(processo_origem)||':'||trim(thread_id)), 2);        
              insert into registro_mata_sessao values (sysdate, 'PROCESS:THREAD = '||trim(trim(processo_origem)||':'||trim(thread_id)), trim(trim(processo_origem)||':'||trim(thread_id)), 1);
              insert into registro_mata_sessao values (sysdate, 'SID = '||x.valor_sid, trim(trim(processo_origem)||':'||trim(thread_id)), 2);
              select terminal into valor_maquina from vw_sessao where sid = x.valor_sid;
              insert into registro_mata_sessao values (sysdate, 'MAQUINA = '||valor_maquina, trim(trim(processo_origem)||':'||trim(thread_id)), 3);
              insert into registro_mata_sessao values (sysdate, 'TIMESTAMP;SCHEMA;SID;USERNAME;SCHEMANAME;OSUSER;PROCESS;MACHINE;PROGRAM;TYPE;LOGON_TIME;MODULE;LAST_CALL_ET;SQL_ID;STATUS;WAIT_CLASS;STATE;SECONDS_IN_WAIT', trim(trim(processo_origem)||':'||trim(thread_id)), 4);
              ordem:=5;
              For Reg_Linha in (select SID||';'||USERNAME||';'||SCHEMANAME||';'||OSUSER||';'||PROCESS||';'||MACHINE||';'||PROGRAM||';'||TYPE||';'||to_char(LOGON_TIME, 'YYYYMMDD_HH24MISS')||';'||MODULE||';'||LAST_CALL_ET||';'||SQL_ID||';'||STATUS||';'||WAIT_CLASS||';'||STATE||';'||SECONDS_IN_WAIT linha from v$session) Loop
                 insert into registro_mata_sessao values (sysdate, valor_identificador||';'||Reg_Linha.linha, trim(trim(processo_origem)||':'||trim(thread_id)), ordem);
                 ordem:=ordem+1;
              End Loop;
              insert into registro_mata_sessao values (sysdate, 'TIMESTAMP;SCHEMA;SID;PIECE;SQL_TEXT', trim(trim(processo_origem)||':'||trim(thread_id)), ordem);
              ordem:=ordem+1;
              For Reg_Linha in (SELECT /*+ INDEX (c) INDEX (q) USE_NL (q) */ x.valor_sid||';'||q.piece||';'||q.sql_text LINHA FROM v$open_cursor c, v$sqltext_with_newlines q WHERE c.sid = x.valor_sid AND c."ADDRESS" = q."ADDRESS" AND c."HASH_VALUE" = q."HASH_VALUE" ORDER BY q."ADDRESS", q."HASH_VALUE", q.piece) Loop
                insert into registro_mata_sessao values (sysdate, valor_identificador||';'||reg_linha.linha, trim(trim(processo_origem)||':'||trim(thread_id)), ordem);
                ordem:=ordem+1;
              End Loop;
              insert into registro_mata_sessao values (sysdate, 'TIMESTAMP;SCHEMA;HORA_LOGON;SID;SERIAL#;V$SESSION.STATUS;PROGRAM;USERNAME;COMMAND;OSUSER;PROCESS;MACHINE;OBJECT_NAME;PGA Memory, in MB;CPU, used by session;Hard Parse, %;Physical read bytes, in MB;Physical write bytes, in MB;Redo size, in MB;Received from client, in MB;Sent to client, in MB', trim(trim(processo_origem)||':'||trim(thread_id)), ordem);
              ordem:=ordem+1;
              For Reg_Linha in (SELECT to_char(Logon_time,'YYYYMMDD HH24MISS')||';'||SID||';'||SERIAL#||';'||v$session.STATUS||';'||PROGRAM||';'||USERNAME||';'||DECODE(command, 0, 'Nenhum', 2, 'Insert', 3, 'Select', 6, 'Update', 7, 'Delete', 8, 'Drop', COMMAND)||';'||OSUSER||';'||PROCESS||';'||MACHINE||';'||OBJECT_NAME||';'||(SELECT ROUND (VALUE / 1024 / 1024, 2) FROM v$sesstat WHERE STATISTIC# = 25 AND v$sesstat.SID = v$session.sid)||';'||(SELECT VALUE FROM v$sesstat WHERE STATISTIC# = 12 AND v$sesstat.SID = v$session.sid)||';'||ROUND ( (SELECT VALUE FROM v$sesstat WHERE STATISTIC# = 339 AND v$sesstat.SID = v$session.sid)/ (SELECT DECODE (VALUE, 0, 1, VALUE) FROM v$sesstat WHERE STATISTIC# = 338 AND v$sesstat.SID = v$session.sid),2)||';'||(SELECT ROUND (VALUE / 1024 / 1024, 2) FROM v$sesstat WHERE STATISTIC# = 58 AND v$sesstat.SID = v$session.sid)||';'||(SELECT ROUND (VALUE / 1024 / 1024, 2) FROM v$sesstat WHERE STATISTIC# = 66 AND v$sesstat.SID = v$session.sid)||';'||(SELECT ROUND (VALUE / 1024 / 1024, 2) FROM v$sesstat WHERE STATISTIC# = 139 AND v$sesstat.SID = v$session.sid)||';'||(SELECT ROUND (VALUE / 1024 / 1024, 2) FROM v$sesstat WHERE STATISTIC# = 344 AND v$sesstat.SID = v$session.sid)||';'||(SELECT ROUND (VALUE / 1024 / 1024, 2) FROM v$sesstat WHERE STATISTIC# = 343 AND v$sesstat.SID = v$session.sid) LINHA FROM v$session LEFT OUTER JOIN DBA_OBJECTS ON v$session.ROW_WAIT_OBJ# = dba_objects.object_ID WHERE v$session.LOGON_TIME BETWEEN TRUNC (SYSDATE) AND SYSDATE ORDER BY sid) Loop
                insert into registro_mata_sessao values (sysdate, valor_identificador||';'||reg_linha.linha, trim(trim(processo_origem)||':'||trim(thread_id)), ordem);
                ordem:=ordem+1;
              End Loop;
              commit;
              select inst_id, SYS_CONTEXT('USERENV', 'INSTANCE'), process into inst_vitima, inst_local, processo_destino from vw_sessao where sid = x.valor_sid and serial# = valor_serial;
              if (inst_vitima = inst_local)
                  then begin
                      SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - processo vitima: '||processo_destino);
                      SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - mesmo nó (job): '||inst_vitima);
                      SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - comando: '||'execute immediate ''alter system kill session ''''' || x.valor_sid ||',' || valor_serial || ''''' IMMEDIATE'';');
          
                      comando := 'alter system kill session '''||x.valor_sid||','||valor_serial||''' immediate';
                      cur := dbms_sql.open_cursor;
                      dbms_sql.parse(cur,comando,dbms_sql.v7);
                      ret := dbms_sql.execute(cur);
                      dbms_sql.close_cursor(cur);
                    end;
                    else begin
                          SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - processo vitima: '||processo_destino);
                          SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - nó diferente (job): '||inst_vitima);
                          SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - comando: '||'execute immediate ''alter system kill session ''''' || x.valor_sid ||',' || valor_serial || ''''' IMMEDIATE'';');
                          DBMS_JOB.SUBMIT (
                          job => job_no,
                          what => 'execute immediate ''alter system kill session ''''' || x.valor_sid ||',' || valor_serial || ''''' IMMEDIATE'';',
                          INSTANCE => inst_vitima);
                          dbms_job.run(job_no);
                          COMMIT;
                    end;    
                end if;
            end;    
            else
              SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - Linguagem: '||linguagem||' Mata Sessao NAO executado!');
            end if;
        end loop;        
        exception
        when others then
          begin
              SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - exception ');
              SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - erro: '|| sqlerrm);              
              dbms_job.remove(job_no);
              raise_application_error( -20050 , 'erro: '|| sqlerrm);
          end;
        if dbms_sql.is_open(cur) then
           SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - Fecha Cursor.'); 
           dbms_sql.close_cursor(cur);
        end if;
    end;
    else begin
          SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - NAO achou PROCESSO de DESTINO correspondente a '||trim(processo_origem)||':'||trim(thread_id));
          raise_application_error( -20050 , 'NAO achou PROCESSO de DESTINO correspondente a '||trim(processo_origem)||':'||trim(thread_id));
    end;
  end if;
  SYS.DBMS_SYSTEM.KSDWRT(2,'Mata Sessao COBOL - PROCESSO: '||trim(trim(processo_origem)||':'||trim(thread_id))||' - FIM: '||to_char(sysdate, 'YYYYMMDD HH24MISS'));
end;
/

show error;