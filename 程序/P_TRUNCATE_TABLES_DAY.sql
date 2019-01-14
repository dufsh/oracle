--/
CREATE PROCEDURE P_TRUNCATE_TABLES_DAY IS
  V_END_DATE   DATE;
  V_PARTVALUE  DATE;
  V_TABLENAME  VARCHAR2(200);
  V_PARTNAME   VARCHAR2(200);
  V_SQL        VARCHAR2(2000);
  V_HIGH_VALUE VARCHAR2(2000);
  V_MONTH      NUMBER;
  V_CURR_DATE  DATE := SYSDATE;
  V_ERR        VARCHAR2(200);

  /*ÿ��1�㶨ʱ������T_TRUNCATE_TABLES_DAY�б������*/
BEGIN

  UPDATE T_TRUNCATE_TABLES_DAY SET SQLTEXT = NULL, TRUN_DATE = NULL;
  COMMIT;

  FOR CUR_TABLES IN (SELECT TABLE_NAME, TYPE_CODE, TYPE_NUM, PART
                       FROM T_TRUNCATE_TABLES_DAY
                      WHERE ISDELETE = 1) LOOP
  
    V_MONTH     := CUR_TABLES.TYPE_NUM;
    V_TABLENAME := CUR_TABLES.TABLE_NAME;
  
    --ȡ����Ҫ��������ݵ����ڷ�Χ
    IF CUR_TABLES.TYPE_CODE = 'MONTH' THEN
      SELECT TRUNC(ADD_MONTHS(V_CURR_DATE, -V_MONTH))
        INTO V_END_DATE
        FROM DUAL;
    ELSIF CUR_TABLES.TYPE_CODE = 'DAY' THEN
      SELECT TRUNC(V_CURR_DATE - V_MONTH) INTO V_END_DATE FROM DUAL;
    END IF;
  
    --ɾ������������
    IF CUR_TABLES.PART = 0 THEN
      --ȡ�÷�����ķ�������
      FOR CUR_PARTNAME IN (SELECT PARTITION_NAME, HIGH_VALUE
                             FROM USER_TAB_PARTITIONS
                            WHERE TABLE_NAME = V_TABLENAME
                              AND (PARTITION_NAME LIKE 'SYS_P%' 
                              --OR PARTITION_NAME LIKE 'FM_ALARM_HISTORY%'
                                  )
                            ORDER BY PARTITION_NAME) LOOP
      
        V_HIGH_VALUE := CUR_PARTNAME.HIGH_VALUE;
        V_PARTVALUE  := TO_DATE(SUBSTR(V_HIGH_VALUE, 11, 10), 'YYYY-MM-DD');
        V_PARTNAME   := CUR_PARTNAME.PARTITION_NAME;
      
        
        IF V_PARTVALUE <= V_END_DATE then
        
          --����������2�������10��
          FOR I IN 1 .. 20 LOOP
            BEGIN
              V_SQL := 'ALTER TABLE ' || V_TABLENAME || ' DROP PARTITION ' || V_PARTNAME || ' UPDATE INDEXES PARALLEL 3';
            
            
            /*
               add by dufs 2018-10-20 ���� FM_ALARM_HISTORY��FM_ALARM_PERMANENT�����Ƶ�ʺܸߣ��Ҳ����г�������Ҫд��
              ���ֱ��drop���ڵķ�����д�볬������ʱҪ���������������������� FM_ALARM_HISTORY ÿ��Ҫɾ���Ĵﵽ400�����ɾ��400����󣬻�Ҫ½����������
              �����Ϊtruncate���������������������۵ķ�����Խ��Խ�࣬ÿ�β�����ʱ�������Խ��Խ����
              �ĳɷ�������ʱ�������ʱ����棬��ÿ��ֻ��պó��ڵ���һ���������ȿ��Դﵽ��շ������ٴ洢���ݵ�Ŀ�꣬���ܽ��Ͳ����������Ӽ��ٴβ������ٵ�1��
            */
              if V_TABLENAME='FM_ALARM_HISTORY' or V_TABLENAME='FM_ALARM_PERMANENT'
              then
                     --dbms_output.put_line('V_PARTVALUE '||to_char(V_PARTVALUE,'yyyy-mm-dd hh24:mi:ss'));
                     --dbms_output.put_line('V_END_DATE-1'||to_char(V_END_DATE-1,'yyyy-mm-dd hh24:mi:ss'));

                     if V_PARTVALUE <= V_END_DATE-1
                     then
                        exit;
                     end if;

                     V_SQL := 'ALTER TABLE ' || V_TABLENAME || ' TRUNCATE PARTITION ' || V_PARTNAME || ' UPDATE GLOBAL INDEXES PARALLEL 3';
              end if;
              
              dbms_output.put_line(V_SQL);
              
              EXECUTE IMMEDIATE V_SQL;
            
              UPDATE T_TRUNCATE_TABLES_DAY
                 SET SQLTEXT   =  V_PARTNAME ,
                     TRUN_DATE = V_CURR_DATE
               WHERE TABLE_NAME = V_TABLENAME;
              COMMIT;
              EXIT;
            EXCEPTION
              WHEN OTHERS THEN
                V_ERR := SUBSTR(SQLERRM, 1, 200);
                IF SUBSTR(V_ERR, 1, 9) = 'ORA-00054' THEN
                  IF I = 10 THEN
                    INSERT INTO T_ISS_SQLERRM VALUES (SEQ_T_ISS_SQLERRM.NEXTVAL,'P_TRUNCATE_TABLES_DAY',V_ERR,SYSDATE);
                    COMMIT;
                  END IF;
                  DBMS_LOCK.SLEEP(2);
                  CONTINUE;
                ELSE
                  INSERT INTO T_ISS_SQLERRM VALUES (SEQ_T_ISS_SQLERRM.NEXTVAL,'P_TRUNCATE_TABLES_DAY',V_ERR,SYSDATE);
                  COMMIT;
                  EXIT;
                END IF;
            END;
          
          END LOOP;
        END IF;
      
      END LOOP;
    
    END IF;
  
  END LOOP;


EXCEPTION
  WHEN OTHERS THEN
    V_ERR := SUBSTR(SQLERRM, 1, 200);
    ROLLBACK;
    INSERT INTO T_ISS_SQLERRM
    VALUES
      (SEQ_T_ISS_SQLERRM.NEXTVAL, 'P_TRUNCATE_TABLES_DAY', V_ERR, SYSDATE);
    COMMIT;
 
END;

/
