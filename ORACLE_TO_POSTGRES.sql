set serveroutput on;
clear screen;

declare
    var_contexto varchar2(300) := sys_context('userenv', 'current_schema'); 
	accept origem char prompt 'Informe o SCHEMA de origem: '
	accept destino char prompt 'Informe o SCHEMA de destino: '
    schema_origem varchar2(3000):='&origem';
	schema_destino varchar2(3000):='&destino';   
	-- nome do dblink que será usado para conectar com o banco de destino (postgres)
    nome_dblink varchar2(300) := '@ORA_PG';
	-- nome do banco de dados de destino (no postgres)
    db_destino varchar2(300):='postgres';
	
	--retorna somentes as tabelas comuns aos dois bancos
	cursor cr_lista_tab is select table_name 
                        from dba_tables 
                        where lower(table_name) in (select distinct "table_name"
                                            from "public"."vw_schema"@ORA_PG
                                            where "table_schema" = schema_destino
                                            )
                        and owner = var_contexto
                        order by num_rows desc;
	--lista de campos especiais de cada tabela
    cursor cr_campos_clob (nome_tabela varchar2) is select column_name 
                                                    from dba_tab_cols 
                                                    where table_name = nome_tabela 
                                                    and data_type in ('CLOB', 'LONG', 'LONG RAW')
                                                    and owner = var_contexto
                                                    order by column_id;
    cmd_insert clob;
    corpo_insert clob;
    comando clob;
    total number:=0;
    cont number;
    maior number;
    nome_tabela varchar2(300);
    nr_migracao number;
    hora_inicio varchar2(30);
    hora_fim varchar2(30);
	total_clob number;
	total_origem number;
    total_view number;
    total_destino number;
    total_linhas_lidas number :=0;
    total_linhas_importadas number :=0;
    total_tabelas_lidas number :=0;
    total_tabelas_importadas number :=0;
    total_advertencias number :=0;
	filtrado char(1);
	vlr_status varchar2(3000);

begin
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
	--altera o formato de data para o formato padrão do POSTGRESQL
    execute immediate 'alter session set nls_date_format = ''DD/MM/YYYY HH24:MI:SS''';
        hora_inicio:=sysdate;
	--registra o inicio da migração
    insert into "public"."tb_acompanhamento"@ORA_PG ("inicio", "schema_origem", "schema_destino", "banco_destino",  "status")  
    values (hora_inicio, schema_origem, schema_destino, db_destino,'Inicio da Migração');
    commit;
	-- pega o número de referência da migração, esse número será usado como chave para registrar as ações da própria migração na tabela de acompanhamento
    select "num_migracao" into nr_migracao from "public"."tb_acompanhamento"@ORA_PG where "inicio" = hora_inicio;
	
	-- mostra os dados de inicio da migração para o usuário
    dbms_output.put_line('Numero da Migração: '||nr_migracao||chr(10));
    dbms_output.put_line('Inicio da Migração: '||to_char(sysdate, 'DD/MM/YYYY HH24:MI:SS'));    

	-- incia o processo para cada tabela
    for tabela in cr_lista_tab loop
		nome_tabela := tabela.table_name;
		filtrado := 'n';
		--mostra para o usuário o nome da tabela corrente
		dbms_output.put_line(nome_tabela);
		--mostra para o usuário o inicio da migração da tabela corrente
        dbms_output.put_line('Inicio:'||to_char(sysdate, 'DD/MM/YYYY HH24:MI:SS'));
		
		--registra o inicio da migração da tabela corrente na tabela de acompanhamento
		hora_inicio:=sysdate;
        insert into "public"."tb_acompanhamento"@ORA_PG ("num_migracao", "inicio", "schema_origem", "schema_destino", "banco_destino",  "tabela", "status")  
        values (nr_migracao, hora_inicio, schema_origem, schema_destino, db_destino, nome_tabela, 'Lendo tabela de origem ...');
        COMMIT;
		--verifica a qde de linhas na tabela de origem 
		execute immediate 'select count(*) from '||nome_tabela into total_origem;    
		dbms_output.put_line('Numero de linhas: '||total_origem);

		-- registra o total de registros na tabela de acompanhamento
		update "public"."tb_acompanhamento"@ORA_PG set  "num_linhas_origem" = total_origem
		where "num_migracao" = nr_migracao and "tabela" = tabela.table_name;
		commit;

		--contabiliza a qde de linhas e tabelas que serão usadas no relatório da migração
        total_linhas_lidas:=total_linhas_lidas+total_origem;
        total_tabelas_lidas:=total_tabelas_lidas+1;
		--verifica se a tabela de destino está vazia
        execute immediate 'select count(*)  from "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink into total_destino;
		
		--se a tabela de origem tem registros e a tabela de destino está vazia
		--então continua a migração
        if total_origem > 0 and total_destino = 0
        then
			-- verifica se a tabela corrente tem campos LONG
			select count(*) into total from dba_tab_cols where data_type like 'LONG%' and table_name = tabela.table_name and owner = var_contexto;
            if total > 0
            then 
				--verifica se a tabela auxiliar TB_AUX_||nr_migracao existe
				-- em caso afirmativo excluir
				select count(*) into total from dba_tables where table_name = 'TB_AUX_'||nr_migracao and owner = var_contexto;
                if total > 0
                    then execute immediate 'drop table TB_AUX_'||nr_migracao;
                end if;
				--cria a tabela auxiliar TB_AUX_||nr_migracao com a mesma estrutura da tabela corrente
                select 'create table TB_AUX_'||nr_migracao||' as select' || t  || ' ' into comando
                from (select replace (converti,', from', ' from') t from  (  SELECT  ' ' ||(
                select  xmlagg(xmlparse(content case 
                  when DATA_TYPE like 'LONG%' then  'to_lob(' || COLUMN_NAME || ')' || COLUMN_NAME
                else COLUMN_NAME END || ',' wellformed )ORDER BY COLUMN_ID).getclobval() 
                FROM dba_tab_columns where table_name = nome_tabela and owner = var_contexto GROUP BY 1 )  || ' from '||nome_tabela as converti 
                FROM dba_tab_columns where table_name = nome_tabela and owner = var_contexto and rownum= 1 group by 1) conveti);
                execute immediate comando;
				--altera o nome da tabela corrente para a tabela auxiliar
				-- isso é necessário para referenciar a tabela convertida e não a original
                nome_tabela := 'TB_AUX_'||nr_migracao;
            end if;    
            
			--verifica se a view auxiliar VW_AUX_||nr_migracao existe
			-- em caso afirmativo excluir
			select count(*) into total from dba_views where view_name = 'VW_AUX_'||nr_migracao and owner = var_contexto;
			if total > 0
				then execute immediate 'drop view VW_AUX_'||nr_migracao;
			end if;                
			
			--cria a view auxiliar para a conversão dos campos BLOB
            select 'create view VW_AUX_'||nr_migracao||' as select' || t  || ' ' into comando
            from (select replace (converti,', from', ' from') t from  (  SELECT  ' ' ||(
            select  xmlagg(xmlparse(content case when DATA_TYPE = 'BLOB' then 'dbms_lob.substr('||COLUMN_NAME || ',2000,1)' || COLUMN_NAME 
            else COLUMN_NAME END || ',' wellformed )ORDER BY COLUMN_ID).getclobval() 
            FROM dba_tab_columns where table_name = nome_tabela and owner = var_contexto GROUP BY 1 )  || ' from '||nome_tabela as converti 
            FROM dba_tab_columns where table_name = nome_tabela and owner = var_contexto and rownum = 1 group by 1) conveti);
            
            total_clob := 0;
            for x in cr_campos_clob(upper(tabela.table_name)) loop
                if total_clob = 0
                 then comando:=comando||' where ';
                 else comando:=comando||' and ';  
                end if;
                comando:=comando||' (length('||x.column_name||')<= 32767 or '||x.column_name||' is null)';
                total_clob:=1;
				filtrado:= 's';
            end loop;

            execute immediate comando;
			
			select count(*) into total_clob from dba_tab_cols where data_type in ('CLOB', 'LONG', 'LONG RAW') and table_name = tabela.table_name and owner = var_contexto;
            if total_clob = 0
            then 
                -- monta o comando de insert que será usado na migração SEM campos CLOB
				select replace(valores, ',)', ')') into cmd_insert
				from 
				(SELECT  'INSERT INTO "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink||' (' ||
					(select  xmlagg(xmlparse(content lower('"'||COLUMN_NAME||'"') || ',' wellformed )ORDER BY COLUMN_ID).getclobval() 
					FROM dba_tab_columns 
					where lower(table_name) = lower(tabela.table_name)
					and lower(column_name) in (select "column_name"
										from "public"."vw_schema"@ORA_PG
										where "table_name" = lower(tabela.table_name))
					and data_type not in ('LONG RAW', 'LONG', 'CLOB', 'BINARY_DOUBLE', 'RAW', 'BLOB')
                    and owner = var_contexto
                    GROUP BY 1 
					) 
					|| ') VALUES (' 
					||(select  xmlagg(xmlparse(content  'x.'||COLUMN_NAME || ',' wellformed )ORDER BY COLUMN_ID).getclobval()||')' 
					FROM dba_tab_columns 
					where lower(table_name) = lower(tabela.table_name)
					and lower(column_name) in (select "column_name"
										from "public"."vw_schema"@ORA_PG
										where "table_name" = lower(tabela.table_name))
					and data_type not in ('LONG RAW', 'LONG', 'CLOB', 'BINARY_DOUBLE', 'RAW', 'BLOB')
                    and owner = var_contexto
                    GROUP BY 1 
					  ) valores
					  from dba_tables where lower(table_name) = lower(nome_tabela)
                      and owner = var_contexto
				);
			commit;	
            --executa o comando de insert para cada linha retornada
			--bloco anônimo criado em tempo de execução
            execute immediate '
                declare
                cursor cr_comando is select * from VW_AUX_'||nr_migracao||';
                total_destino number;
                nr_migracao number;
                erro varchar2(4000);
                BEGIN
                    nr_migracao := '||nr_migracao||';
					--incia a migração dos registros na tabela corrente
                    update "public"."tb_acompanhamento"'||nome_dblink||' set "status" = ''Em andamento ...'' 
                    where "num_migracao" = nr_migracao and "tabela" = '''||upper(tabela.table_name)||''';
                    commit;
					--insere os registros na tabela de destino
					for x in cr_comando loop
						'||cmd_insert||';                
						commit;
					end loop;
					
					-- conta a qde de linhas importadas
                    update "public"."tb_acompanhamento"'||nome_dblink||' set "status" = ''Verificando linhas na tabela de destino ...'' 
                    where "num_migracao" = nr_migracao and "tabela" = '''||upper(tabela.table_name)||''';
                    commit;
					select count(*) into total_destino from "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink||';
					
					--registra o fim da migração da tabela corrente
					update "public"."tb_acompanhamento"'||nome_dblink||' set "num_linhas_destino" = total_destino, "status" = ''ok''
					where "num_migracao" = nr_migracao and "tabela" = '''||tabela.table_name||''';
					commit;
					
					--mostra para o usuário o total de linhas importadas
					dbms_output.put_line(''Total de linhas importadas: ''||total_destino);
                    EXCEPTION
                      WHEN OTHERS THEN
                        erro := ''Erro: ''||SQLERRM;
                        ROLLBACK;
                        DBMS_OUTPUT.PUT_LINE('''||cmd_insert||''');
                        DBMS_OUTPUT.PUT_LINE(''erro do exception ''||erro);        
                        commit;
						
						--verifica a quantidade de linhas importadas
                        select count(*) into total_destino from "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink||';
						
						--registra o erro na tabela de acompanhamento
                        update "public"."tb_acompanhamento"'||nome_dblink||' set "status" = erro, "num_linhas_destino" = total_destino
                        where "num_migracao" = nr_migracao and "tabela" = '''||upper(tabela.table_name)||''';
                        commit;
                        dbms_output.put_line(''Total de linhas importadas: ''||total_destino);
                end;
            ';
				
			else
				
                -- monta o comando de insert que será usado na migração COM campos CLOB
				-- a diferença é que neste não há fechamento dos parenteses dos campos e dos valores
				-- esses serão realizados apos a inclusão dos campos CLOB por variável no bloco anônimo recorrente
				select valores into cmd_insert
				from 
				(SELECT  'INSERT INTO "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink||' (' ||
					(select  xmlagg(xmlparse(content lower('"'||COLUMN_NAME||'"') || ',' wellformed )ORDER BY COLUMN_ID).getclobval() 
					FROM dba_tab_columns 
					where lower(table_name) = lower(tabela.table_name)
					and lower(column_name) in (select "column_name"
										from "public"."vw_schema"@ORA_PG
										where "table_name" = lower(tabela.table_name))
					and data_type not in ('LONG RAW', 'LONG', 'BINARY_DOUBLE', 'RAW', 'BLOB', 'CLOB')
                    and owner = var_contexto
                    GROUP BY 1 
					)  valores
					  from dba_tables where lower(table_name) = lower(nome_tabela)
                      and owner = var_contexto
				);

				select valores into corpo_insert
				from 
				(SELECT  ' VALUES (' 
					||(select  xmlagg(xmlparse(content  'x.'||COLUMN_NAME || ', ' wellformed )ORDER BY COLUMN_ID).getclobval()
					FROM dba_tab_columns 
					where lower(table_name) = lower(tabela.table_name)
					and lower(column_name) in (select "column_name"
										from "public"."vw_schema"@ORA_PG
										where "table_name" = lower(tabela.table_name))
					and data_type not in ('LONG RAW', 'LONG', 'BINARY_DOUBLE', 'RAW', 'BLOB', 'CLOB')
                    and owner = var_contexto
                    GROUP BY 1 
					  ) valores
					  from dba_tables where lower(table_name) = lower(nome_tabela)
                      and owner = var_contexto
				);
                
                for x in cr_campos_clob(upper(tabela.table_name)) loop
                    cmd_insert:=cmd_insert||'"'||lower(x.column_name)||'",';
                end loop;
				cmd_insert := replace(cmd_insert||')', ',)', ')');
                for x in 1 .. total_clob loop
                    corpo_insert := corpo_insert||' var_'||x||', ';
                end loop;
                cmd_insert := cmd_insert||corpo_insert;
                cmd_insert := replace(cmd_insert||')', ', )', ')');
				commit;
                
            --executa o comando de insert para cada linha retornada
			-- bloco anônimo criado em tempo de execução
            comando:= '
                declare
                cursor cr_comando is select * from VW_AUX_'||nr_migracao||';
                total_destino number;
                nr_migracao number;
                erro varchar2(4000);
				';
				for x in 1 .. total_clob loop
					comando:= comando||'var_'||x||' varchar2(32767);
                    ';
				end loop;
				comando := comando||'
                BEGIN
                    nr_migracao := '||nr_migracao||';
					--incia a migração dos registros na tabela corrente
                    update "public"."tb_acompanhamento"'||nome_dblink||' set "status" = ''Em andamento ...'' 
                    where "num_migracao" = nr_migracao and "tabela" = '''||upper(tabela.table_name)||''';
                    commit;                  
					';
					comando:= comando||'
					--insere os registros na tabela de destino
					for x in cr_comando loop
                        ';
                    cont := 1;    
                    -- por causa da limitação do dblink é necessário criar N variáveis suficientes para 
					-- armazenar o conteúdo dos campos LOB maiores de que 32k
                    for x in cr_campos_clob(upper(tabela.table_name)) loop
                        comando:= comando||'var_'||cont||':= x.'||x.column_name||';
                        ';
                        cont:=cont+1;
                    end loop;
                    comando:= comando||'
                    '||cmd_insert;
                    comando:= comando||';
                            commit;
					end loop;
                    
					-- conta a qde de linhas importadas
                    update "public"."tb_acompanhamento"'||nome_dblink||' set "status" = ''Verificando linhas na tabela de destino ...'' 
                    where "num_migracao" = nr_migracao and "tabela" = '''||upper(tabela.table_name)||''';
                    commit;
					select count(*) into total_destino from "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink||';
					
					--registra o fim da migração da tabela corrente
					update "public"."tb_acompanhamento"'||nome_dblink||' set "num_linhas_destino" = total_destino, "status" = ''ok''
					where "num_migracao" = nr_migracao and "tabela" = '''||tabela.table_name||''';
					commit;

					--mostra para o usuário o total de linhas importadas
					dbms_output.put_line(''Total de linhas importadas: ''||total_destino);
                    EXCEPTION
                      WHEN OTHERS THEN
                        erro := ''Erro: ''||SQLERRM;
                        ROLLBACK;
                        DBMS_OUTPUT.PUT_LINE('''||cmd_insert||''');
                        DBMS_OUTPUT.PUT_LINE(''erro do exception ''||erro);        
                        commit;
						
						--verifica a quantidade de linhas importadas
                        select count(*) into total_destino from "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink||';

						--registra o erro na tabela de acompanhamento
                        update "public"."tb_acompanhamento"'||nome_dblink||' set "status" = erro, "num_linhas_destino" = total_destino
                        where "num_migracao" = nr_migracao and "tabela" = '''||upper(tabela.table_name)||''';
                        commit;
                        dbms_output.put_line(''Total de linhas importadas: ''||total_destino);
                end;
            ';

             execute immediate comando;
			end if;
			--verifica a quantidade de linhas importadas
            execute immediate 'select count(*)  from "'||schema_destino||'"."'||lower(tabela.table_name)||'"'||nome_dblink into total_destino;
            total_linhas_importadas:=total_linhas_importadas+total_destino;
            total_tabelas_importadas:=total_tabelas_importadas+1;
        elsif total_origem = 0
			then
				-- a tabela de origem não contém registros
				-- mostra a advertência para o usuário 				
				dbms_output.put_line('Advertência: Não foi importada porque não contém registros na origem.');
				
				--registra a advertência na tabela de acompanhamento
				update "public"."tb_acompanhamento"@ORA_PG set  "num_linhas_destino" = total_destino, "status" = 'Advertência: Não foi importada porque não contém registros na origem.'
				where "num_migracao" = nr_migracao and "tabela" = tabela.table_name;
				commit;
			else
				-- a tabela de destino contém registros
				-- mostra a advertência para o usuário 
				dbms_output.put_line('Advertência: Não foi importada porque já contém registros no destino.');
				
				--registra a advertência na tabela de acompanhamento
				update "public"."tb_acompanhamento"@ORA_PG set  "num_linhas_destino" = total_destino, "status" = 'Advertência: Não foi importada porque já contém registros no destino.'
				where "num_migracao" = nr_migracao and "tabela" = tabela.table_name;
				commit;
        end if;
		
		select "status" into vlr_status from "public"."tb_acompanhamento"@ORA_PG where "num_migracao" = nr_migracao and "tabela" = tabela.table_name;
		--Verifica o resultado da migração da tabela corrente
		if filtrado = 's' and total_origem > 0 and vlr_status = 'ok'
			then 
				if total_origem > total_destino
				then 
					update "public"."tb_acompanhamento"@ORA_PG set  "status" = 'Atenção: Encontrados registros maiores que 32k'
					where "num_migracao" = nr_migracao and "tabela" = tabela.table_name;
				end if;
			else
				if total_origem > total_destino and vlr_status = 'ok'
				then 
					update "public"."tb_acompanhamento"@ORA_PG set  "status" = 'Erro: Nem todos os registros foram importados.'
					where "num_migracao" = nr_migracao and "tabela" = tabela.table_name;
				end if;
        end if;
		--registra o fim da migração da tabela corrente
		hora_fim := sysdate;
		update "public"."tb_acompanhamento"@ORA_PG set  "fim" = hora_fim
		where "num_migracao" = nr_migracao and "tabela" = tabela.table_name;
		commit;
		
		-- mostra para o usuário o fim da migração da tabela
        dbms_output.put_line('Fim:'||to_char(sysdate, 'DD/MM/YYYY HH24:MI:SS')||chr(10));
    end loop;
    --exclui os objetos temporarios
    select count(*) into total from dba_views where view_name = 'VW_AUX_'||nr_migracao and owner = var_contexto;
    if total > 0
        then execute immediate 'drop view VW_AUX_'||nr_migracao;
    end if;                
    select count(*) into total from dba_tables where table_name = 'TAB_AUX_'||nr_migracao and owner = var_contexto;
    if total > 0
        then execute immediate 'drop table TAB_AUX_'||nr_migracao;
    end if;
    hora_fim:=sysdate;
	-- registra na tabela de acompanhamento o fim da migração
    insert into "public"."tb_acompanhamento"@ORA_PG ("num_migracao", "inicio", "schema_origem", "schema_destino", "banco_destino",  "status")  
    values (nr_migracao, hora_fim, schema_origem, schema_destino, db_destino,'Fim da Migração');
	
	-- mostra para o usuário o fim da migração
    dbms_output.put_line('Fim da Migração: '||to_char(sysdate, 'DD/MM/YYYY HH24:MI:SS')||chr(10));
	--gera relatório da migração
    dbms_output.put_line('Relatório de Migração: '||chr(10));
    select (max("fim") - min("inicio"))*86400 into total from "public"."tb_acompanhamento"@ORA_PG where "num_migracao" = nr_migracao;
    dbms_output.put_line('Tempo total (segundos): '||total);

    dbms_output.put_line('Total de tabelas lidas: '||total_tabelas_lidas);
    select count(*) into total_advertencias from "public"."tb_acompanhamento"@ORA_PG where "num_migracao" = nr_migracao and "status" like 'Advert%origem.';
    dbms_output.put_line('Total de tabelas vazias na origem: '||total_advertencias);
    select count(*) into total_advertencias from "public"."tb_acompanhamento"@ORA_PG where "num_migracao" = nr_migracao and "status" like 'Advert%destino.';
    dbms_output.put_line('Total de tabelas com registros no destino: '||total_advertencias);
    select count(*) into total_advertencias from "public"."tb_acompanhamento"@ORA_PG where "num_migracao" = nr_migracao and "status" like 'Erro%';
    dbms_output.put_line('Total de tabelas com erros: '||total_advertencias);
    dbms_output.put_line('Total de tabelas importadas: '||total_tabelas_importadas);
    dbms_output.put_line('Tempo médio de importação de todas as tabela (segundos): '||(total/(total_tabelas_lidas)));
    
    dbms_output.put_line('Total de registros lidos: '||total_linhas_lidas);
	dbms_output.put_line('Tempo médio de importação por registro lido (segundos): '||(total/(total_linhas_lidas)));
    dbms_output.put_line('Total de registros importados: '||total_linhas_importadas);
	if total_linhas_importadas > 0
		then dbms_output.put_line('Tempo médio de importação por registro importado (segundos): '||(total/(total_linhas_importadas)));
		else dbms_output.put_line('Tempo médio de importação por registro importado (segundos): 0');
	end if;    
  
commit;
end;
/
