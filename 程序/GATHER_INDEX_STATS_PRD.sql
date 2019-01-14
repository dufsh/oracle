--/
CREATE PROCEDURE GATHER_INDEX_STATS_PRD
(in_table varchar2, in_indexes IN VARCHAR2)

-- create by dufs 2019-01-10 用于索引的统计信息收集
AS

v_cnt     number;
v_is_part varchar2(32);
v_is_lock varchar2(32);
v_index   varchar2(32);

v_table   varchar2(32);
v_indexes varchar2(4000);

BEGIN

        v_table:=upper(in_table);
        v_indexes:=upper(in_indexes);
        
        select count(*) into v_cnt from user_tables where table_name=v_table;
        if v_cnt = 0 
        then
                dbms_output.put_line('not found table' || v_table);
                return; 
        end if;
        
        if v_indexes is null
        then
                -- 如果传入参数中，索引为空，则取该表所有索引进行统计信息收集
                dbms_output.put_line('v_indexes is null , get all indexes on table ||' || v_table);    
                select listagg(index_name,',') within group(order by index_name) into v_indexes from user_indexes where table_name=v_table;
        end if;
        
        for cur in (select COLUMN_VALUE as index_name from table(str2varlist_new(v_indexes))
        )
        loop
                v_index:=cur.index_name;

                select PARTITIONED into v_is_part from user_indexes where index_name=v_index and table_name=v_table;
        
                if v_is_part is null
                then
                        dbms_output.put_line('not found v_index' || v_index);
                        continue;        
                elsif v_is_part='NO'
                then
                        dbms_output.put_line('v_index ' || v_index || ' is not PARTITIONED');
                        
                        select max(stattype_locked) into v_is_lock from user_tab_statistics where table_name=v_table and stattype_locked is not null;
                        if v_is_lock is not null
                        then
                                dbms_output.put_line('table' || v_table || ' is locked :'||v_is_lock);
                                --表处理锁定状态，需要先解锁                                
                                DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);
                        else
                                dbms_output.put_line('table' || v_table || ' is not locked :'||v_is_lock);
                        end if;
                                        
                        DBMS_STATS.GATHER_index_STATS(OWNNAME     => user,
                                                      indname     => v_index,
                                                      estimate_percent => 10,
                                                      DEGREE      => 3);

                        if v_is_lock is not null
                        then
                                --锁定表的统计信息，避免自动任务分析表
                                DBMS_STATS.LOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);   
                        end if;             
                elsif v_is_part='YES'
                then
                        dbms_output.put_line('index' || v_index || ' is PARTITIONED');
                        select max(stattype_locked) into v_is_lock from user_tab_statistics where table_name=v_table and stattype_locked is not null;
                        if v_is_lock is not null
                        then
                                dbms_output.put_line('table' || v_table || ' is locked :'||v_is_lock);
                                --表处理锁定状态，需要先解锁
                                DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);
                        else
                                dbms_output.put_line('table' || v_table || ' is not locked :'||v_is_lock);
                        end if;
                                
                        for cur_1 in (select partition_name from user_ind_partitions where index_name=v_index
                        )
                        loop
                                DBMS_STATS.GATHER_index_STATS(OWNNAME     => user,
                                                              indname     => v_index,
                                                              PARTNAME    => cur_1.partition_name,
                                                              GRANULARITY => 'PARTITION',
                                                              estimate_percent => 10,
                                                              DEGREE      => 3);
                        end loop; 
                                
                        if v_is_lock is not null
                        then
                                --锁定表的统计信息，避免自动任务分析表
                                DBMS_STATS.LOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);
                        end if;                                 
                else
                        dbms_output.put_line('table' || v_table || ' paritions gived is null');
                end if;
        end loop;
END;
/
