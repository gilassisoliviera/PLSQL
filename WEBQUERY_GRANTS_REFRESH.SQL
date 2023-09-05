create or replace PROCEDURE PR_WEBQUERY_GRANTS_REFRESH
AS
--lista de usuarios que ainda nao foram criados
cursor CR_USER is select distinct 'CREATE USER WQ_'||lgn_bd_usu||' IDENTIFIED BY WQ_'||lgn_bd_usu||' DEFAULT TABLESPACE "WQ_USERS" TEMPORARY TABLESPACE "TEMP"  QUOTA UNLIMITED ON "WQ_USERS" ACCOUNT UNLOCK' CMD
from tabela_login
where 'WQ_'||lgn_bd_usu not in (select username from all_users)
and lgn_bd_usu not like 'WQ_%'
order by 1;

--concessao de privilegios para os novos usuarios
cursor CR_GRANT is select distinct 'GRANT CONNECT, CREATE SESSION TO '||lgn_bd_usu CMD
from tabela_login
where lgn_bd_usu in (select username from all_users)
AND upper('WQ_'||lgn_bd_usu) NOT IN (SELECT USERNAME FROM ALL_USERS)
order by 1;

--concessao de grants nas tabelas do usuario original para o usuario WQ_
CURSOR CR_OBJETOS IS SELECT 'GRANT SELECT ON '||OWNER||'.'||OBJECT_NAME||' TO WQ_'||OWNER CMD
FROM ALL_OBJECTS
WHERE OBJECT_TYPE IN ('TABLE', 'VIEW')
and status = 'VALID'
AND OWNER IN (select distinct lgn_bd_usu from tabela_login
                where lgn_bd_usu in (select username from all_users where username not like 'WQ_%'))
ORDER BY OBJECT_NAME;

--concessao de select para o usuario WQ_ nas mesmas tabelas do usuario original
CURSOR CR_VIEWS IS select 'GRANT SELECT ON '||REFERENCED_OWNER||'.'||REFERENCED_NAME||' TO WQ_'||OWNER CMD
from all_dependencies where type = 'VIEW'
and OWNER IN (select distinct lgn_bd_usu from tabela_login
                where lgn_bd_usu in (select username from all_users where username not like 'WQ_%'))
AND REFERENCED_OWNER not in ('PUBLIC');


--cria as views com o mesmo nome das tabelas do usuario original que nao fazem referencia a objetos de outros usuarios
CURSOR CR_ESPELHA_OBJETOS IS SELECT 'create OR replace view WQ_'||OWNER|| '.'||OBJECT_NAME ||' AS SELECT * FROM '||OWNER||'.'||OBJECT_NAME CMD
FROM ALL_OBJECTS
WHERE OBJECT_TYPE IN ('TABLE', 'VIEW')
AND OWNER IN (select distinct lgn_bd_usu from tabela_login
                where lgn_bd_usu in (select username from all_users where username not like 'WQ_%'))
ORDER BY OBJECT_NAME;

cursor cr_dependencias is select X.owner dono, name nome, TEXT
from all_dependencies X, ALL_VIEWS Y
where type = 'VIEW' AND Y.OWNER = X.OWNER
AND Y.VIEW_NAME = X.NAME
and X.owner in (select distinct lgn_bd_usu from tabela_login where lgn_bd_usu not like 'WQ_%')
and X.owner <> REFERENCED_OWNER
order by 1;

-- lista das views existentes.
cursor cr_exclui is select OWNER, VIEW_NAME FROM ALL_VIEWS WHERE owner like 'WQ%';

cursor cr_compila is select OWNER,OBJECT_NAME FROM ALL_OBJECTS WHERE Status = 'INVALID' AND OBJECT_TYPE = 'VIEW';
BEGIN

DBMS_OUTPUT.ENABLE (buffer_size => NULL);

--exclui todas as views existentes para serem criadas novamente
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--Exclui as views existentes');
  for x in cr_exclui loop
  begin
     DBMS_OUTPUT.PUT_LINE('execute immediate DROP VIEW '||x.OWNER||'.'||x.VIEW_NAME);
     execute immediate 'DROP view '||x.OWNER||'.'||x.VIEW_NAME;
      EXCEPTION
        WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
   end;
 end loop;

--cria usuarios que ainda nao foram criados
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--CRIA USUARIOS');
FOR X IN CR_USER LOOP
begin
  DBMS_OUTPUT.PUT_LINE(X.CMD||';');
  EXECUTE IMMEDIATE X.CMD;
  EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
end;

END LOOP;
--concede permissões aos usuarios criados
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--GRANT CONNECT');
FOR X IN CR_GRANT LOOP
begin
  DBMS_OUTPUT.PUT_LINE(X.CMD||';');
  EXECUTE IMMEDIATE X.CMD;
  EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
end;

END LOOP;

DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--PERMISSAO SOMENTE LEITURA PARA OS OBJETOS DO USUARIO ORIGINAL');
FOR X IN CR_OBJETOS LOOP
begin
  DBMS_OUTPUT.PUT_LINE(X.CMD||';');
  EXECUTE IMMEDIATE X.CMD;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
end;

END LOOP;

DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--PERMISSAO SOMENTE LEITURA PARA OS OBJETOS DO QUAL O USUARIO ORIGINAL TAMBEM TEM PERMISSAO');
FOR X IN CR_VIEWS LOOP
begin
  DBMS_OUTPUT.PUT_LINE(X.CMD||';');
  EXECUTE IMMEDIATE X.CMD;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
end;
END LOOP;


DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--CRIACAO DAS VIEWS REFERENTES AOS OBJETOS DO USUARIO ORIGINAL');
FOR X IN CR_ESPELHA_OBJETOS LOOP
begin
  DBMS_OUTPUT.PUT_LINE(X.CMD||';');
  EXECUTE IMMEDIATE X.CMD;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
end;
END LOOP;

DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--CRIACAO DAS VIEWS REFERENTES AOS OBJETOS NO QUAL O USUARIO ORIGINAL TEM ACESSO');
  for x in cr_dependencias loop
  begin
     DBMS_OUTPUT.PUT_LINE('create OR REPLACE view WQ_'||x.dono||'.'||x.nome||' as '||x.text||' with read only;');
     execute immediate 'create OR REPLACE view WQ_'||x.dono||'.'||x.nome||' as '||x.text;
      EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
   end;
  end loop;


DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('--COMPILA AS VIEWS INVALIDAS');
  for x in cr_compila loop
  begin
     DBMS_OUTPUT.PUT_LINE('execute immediate alter view '||x.OWNER||'.'||x.OBJECT_NAME||' compile');
     execute immediate 'alter view '||x.OWNER||'.'||x.OBJECT_NAME||' compile';
      EXCEPTION
        WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Mensagem Oracle: ' || SQLERRM);
   end;
  end loop;

END;
/