--/
CREATE PROCEDURE GATHER_TABLE_STATS_PRD
(in_table IN VARCHAR2, in_partitions IN VARCHAR2)

-- create by dufs 2019-01-10 用于表的统计信息收集

AS

v_is_part varchar2(32);
v_is_lock varchar2(32);


v_table   varchar2(32);
v_partitions varchar2(4000);

BEGIN
        
        v_table:=upper(in_table);
        v_partitions:=upper(in_partitions);
        
        select PARTITIONED into v_is_part from user_tables where table_name=v_table;
        
        if v_is_part is null
        then
                dbms_output.put_line('not found table' || v_table);
                return;        
        elsif v_is_part='NO'
        then
                dbms_output.put_line('table' || v_table || ' is not PARTITIONED');
                select stattype_locked into v_is_lock from user_tab_statistics where table_name=v_table;
                if v_is_lock is not null
                then
                        dbms_output.put_line('table' || v_table || ' is locked :'||v_is_lock);
                        --表处理锁定状态，需要先解锁
                        DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);                  
                else
                        dbms_output.put_line('table' || v_table || ' is not locked :'||v_is_lock);
                end if;

                DBMS_STATS.GATHER_TABLE_STATS(OWNNAME     => user,
                                        TABNAME     => v_table,
                                        estimate_percent => 10,
                                        DEGREE      => 3);
                                        
                if v_is_lock is not null
                then
                        --锁定表的统计信息，避免自动任务分析表
                        DBMS_STATS.LOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);
                end if;                
        elsif v_is_part='YES'
        then
                dbms_output.put_line('table' || v_table || ' is PARTITIONED');
                if v_partitions is not null
                then
                        select max(stattype_locked) into v_is_lock from user_tab_statistics where table_name=v_table and stattype_locked is not null;
                        if v_is_lock is not null
                        then
                                dbms_output.put_line('table' || v_table || ' is locked :'||v_is_lock);
                                --表处理锁定状态，需要先解锁
                                DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);
                        else
                                dbms_output.put_line('table' || v_table || ' is not locked :'||v_is_lock);
                        end if; 
                        
                        for cur in ( select partition_name from user_tab_partitions where table_name=v_table and  partition_name in (select COLUMN_VALUE from table(str2varlist_new(v_partitions)))
                        )loop 
                                DBMS_STATS.GATHER_TABLE_STATS(OWNNAME     => user,
                                                TABNAME     => v_table,
                                                PARTNAME    => cur.partition_name,
                                                GRANULARITY => 'PARTITION',
                                                DEGREE      => 3,
                                                CASCADE     => TRUE);
                        end loop;                          

                        if v_is_lock is not null
                        then
                                --锁定表的统计信息，避免自动任务分析表
                                DBMS_STATS.LOCK_TABLE_STATS(OWNNAME => user, TABNAME => v_table);
                        end if;                                 
                else
                        dbms_output.put_line('table' || v_table || ' paritions gived is null');
                end if;
        end if;
END;
/
