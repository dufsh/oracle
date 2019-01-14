--/
CREATE PROCEDURE FM_FREQ_PRD
AS

-- create by duf 2017-11-30   ��Ʒ����÷
--�Ĵ�_20171115_��������EPC�ش����Ԥ�����������ĵ�V1.3 �����½ڵ� ��ͬһ��Ԫ30������3�μ��������ϣ�ÿ�������������ڵ���10���� �ж�
--3.1.3.5	������������
--3.1.3.7	���ų�����

-- ����Ҫ��30����Ѳһ�Σ�sch job ����

v_time date:=sysdate;
v_title varchar2(128);
v_ne_id varchar2(128);
v_first_time date;
v_max_event_id number;

v_device_class varchar2(64);
v_first_time_cur date;  -- ����ʱ��-��ǰ

v_cnt number;   -- 1�������ڶϵĸ澯����
v_rn number;    --�澯����
v_num number:=0;--���ϴ����ƴ�

BEGIN

        
        dbms_output.put_line('v_time='||to_char(v_time,'yyyy-mm-dd hh24:mi:ss'));
        for cur_1 in (
        
                -- ֻ��Ҫ�澯 ��SIPFEP��·�жϣ���������澯�٣���ʱ����һ���澯 �澯ָʾ�ź�(LP-AIS) ����֤
                select * from 
                (
                        select count(*) over(partition by e.ne_id,e.title,e.device_class) cnt,
                        row_number() over(partition by e.ne_id,e.title,e.device_class order by e.first_occurrence_time) rn,
                        max(e.event_id) over(partition by e.ne_id,e.title,e.device_class) max_event_id,
                        e.*
                        from fm_alarm_recent e
                        where e.is_cleared=1
                        and e.title in ('��SIPFEP��·�ж�')
                        and e.first_occurrence_time>=v_time-30/1440
                        and e.first_occurrence_time<v_time
                        and e.first_occurrence_time>e.clear_time-1/1440
                        and e.construct_alarm_flag=0
                        and e.device_class in ('SCP','����','SDH')
                        and not exists (select 1 from fm_alarm_freq f where e.ne_id=f.ne_id and e.title=f.title and e.device_class=f.device_class and e.first_occurrence_time<=f.first_occurrence_time)
                ) where cnt>=30
                order by ne_id,title,rn
        )loop
                v_title:=cur_1.title;
                v_ne_id:=cur_1.ne_id;
                v_device_class:=cur_1.device_class;
                v_max_event_id:=cur_1.max_event_id;
                v_rn:=cur_1.rn;
                v_first_time:=cur_1.first_occurrence_time;
                
                if v_rn=1
                then 
                        -- ���ϼƴγ�ʼ��
                        v_num:=0;
                        v_first_time_cur:=v_first_time;
                      --  dbms_output.put_line('new ne_id --------------------------');
                        dbms_output.put_line('ne_id='||v_ne_id);

                        -- �µ�ne_id+title �澯�鿪ʼ
                        select count(*) into v_cnt
                        from fm_alarm_recent e
                        where e.is_cleared=1 
                        and e.title=v_title
                        and e.ne_id=v_ne_id
                        and e.first_occurrence_time>=v_time-30/1440
                        and e.first_occurrence_time<v_time
                        and e.first_occurrence_time>e.clear_time-1/1440
                        and e.first_occurrence_time>=v_first_time
                        and e.first_occurrence_time<=v_first_time+1/1440;
                        
                        if v_cnt>=10
                        then
                             dbms_output.put_line(to_char(v_first_time,'yyyy-mm-dd hh24:mi:ss')||'-'||to_char(v_first_time+1/1440,'yyyy-mm-dd hh24:mi:ss')||'  1���������ϴﵽ'||v_cnt||'��������1��');
                             v_first_time_cur:=v_first_time+1/1440;
                             v_num:=v_num+1;
                        end if;
                 else
                        
                        if v_num>=3
                        then
                            continue;
                        end if;
                        
                        if v_first_time_cur>=v_first_time
                        then 
                        -- ������Ѿ�����Ϊ���ϵĸ澯
                            continue;
                        else
                            -- �����û�б���Ϊ���ϵĸ澯�����°�����澯��ʱ����ͳ��1���������ϴ������Ƿ�ﵽҪ��
                                select count(*) into v_cnt
                                from fm_alarm_recent e
                                where e.is_cleared=1 
                                and e.title=v_title
                                and e.ne_id=v_ne_id
                                and e.first_occurrence_time>=v_time-30/1440
                                and e.first_occurrence_time<v_time
                                and e.first_occurrence_time>e.clear_time-1/1440
                                and e.first_occurrence_time>=v_first_time
                                and e.first_occurrence_time<=v_first_time+1/1440;
                                
                                if v_cnt>=10
                                then
                                     dbms_output.put_line(to_char(v_first_time,'yyyy-mm-dd hh24:mi:ss')||'-'||to_char(v_first_time+1/1440,'yyyy-mm-dd hh24:mi:ss')||'  1���������ϴﵽ'||v_cnt||'��������1��');
                                     v_first_time_cur:=v_first_time+1/1440;
                                     v_num:=v_num+1;
                                     if v_num=3 
                                     then
                                                dbms_output.put_line('���ϴ����Ѵﵽ3��');
                                                insert into fm_alarm_freq(ne_id,title,first_occurrence_time,device_class,event_id) values (v_ne_id,v_title,v_time,v_device_class,v_max_event_id);
                                                commit;
                                                v_num:=v_num+1;
                                                continue;
                                     end if;
                                     
                                end if;
                        end if;
                  end if;
        end loop;
                
        -- ������ϸ澯
        update fm_alarm_freq e set e.is_cleared=1,e.clear_time=sysdate
        where not exists (select 1 from fm_alarm_freq e1 where e.ne_id=e1.ne_id and e.title=e1.title and e1.first_occurrence_time>sysdate-30/1440)
        and e.is_cleared=0;
        commit;
        
        -- ֻ����3�������
        delete from fm_alarm_freq where first_occurrence_time<trunc(sysdate-3);
        commit;
        
END;
/
