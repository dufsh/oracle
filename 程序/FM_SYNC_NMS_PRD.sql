--/
CREATE PROCEDURE FM_SYNC_NMS_PRD
(v_nms  in varchar2)
AS

-- create by dufs 2018-8-22 
-- ���÷�ʽ��211.4�������Ͻű�����
/*
#=============== create by dufs 2018-8-20 ����רҵ������OMC�澯�Ա����飬�������ˣ�Ѧ�� ======================
10 * * * * . $HOME/.profile;cd /backup/fm_sync; ./fm_sync_pn_Run.sh
10 * * * * . $HOME/.profile;cd /backup/fm_sync; ./fm_sync_tn_Run.sh
#=============== create by dufs 2018-8-20 ����רҵ������OMC�澯�Ա����飬�������ˣ�Ѧ�� ======================
*/


v_cnt number:=0;
v_report_time date;
v_omcList varchar2(2000);

v_sql varchar2(4000);

v_prd varchar2(64):='FM_SYNC_NMS_PRD';
v_logInfo varchar2(500);

procedure FM_SYNC_NMS_COMPARE_PRD(v_report_time in date,v_nms_name in varchar2,v_specialty in varchar2)
as
    begin
                -- �õ�omc�б�����Щomc��Χ�ڱȽϸ澯��ԭ��רҵ���ܴ��ڲ���omc�澯ͬ��ʧ�ܵ�ԭ�����������omc��Χ�������ּ��й������Ի��ܶ�澯�����
                select listagg(omc_id,',') within group(order by omc_id) into v_omcList
                from (
                        select distinct d.omc_id
                        from fm_sync_nms_detail d
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                     );
                
                --�����ñ�����Ҫ�ȶԵĸ澯������뼯�й��ϸ澯���бȽ�         
                for cur in (select synctype,titles,device_key,device_value from fm_sync_config where specialty=v_specialty order by order_desc)
                loop
                        dbms_output.put_line(cur.synctype);
                        -- ��ʶһ�µĸ澯������жԱ�
                        update fm_sync_nms_detail d set d.compare_type_jzgz='���й�����'||v_nms_name||'һ��',
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 and rownum=1)
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 /*and e.ne_time=d.ne_time*/)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;
                        commit;
                        
                        update fm_sync_nms_detail d set d.compare_type_jzgz='���й�����'||v_nms_name||'һ��',
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm_permanent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 and rownum=1) 
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm_permanent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 /*and e.ne_time=d.ne_time*/)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;
                        commit;  
                        
                        update fm_sync_nms_detail d set d.compare_type_jzgz='���й�����'||v_nms_name||'һ��', 
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm_recent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 and rownum=1) 
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm_recent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 /*and e.ne_time=d.ne_time*/)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;
                        commit;                        

                        -- ��ʶһ�µĸ澯����ʷ���жԱ�
                        update fm_sync_nms_detail d set d.compare_type_jzgz='���й�����'||v_nms_name||'һ��',
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm_history e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and e.is_derivative=0 and rownum=1)  
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm_history e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and e.is_derivative=0)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;                        
                        commit;

                        -- ��ʶ���й����ٵĸ澯   ɸѡnms_alarm_id��Ϊ�յ����ݣ������б�ʶΪ��OMC��רҵ����һ�£���רҵ������ˮ��Ϊ�յ����
                        update fm_sync_nms_detail d set d.compare_type_jzgz='���й��ϱ�'||v_nms_name||'��' 
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          --and not exists (select 1 from fm_alarm e where e.ems_alarm_id=d.nms_alarm_id /*and e.ne_time=d.ne_time*/)
                          --and not exists (select 1 from fm_alarm_recent e where e.ems_alarm_id=d.nms_alarm_id /*and e.ne_time=d.ne_time*/)
                          --and not exists (select 1 from fm_alarm_history e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;
                        commit;

                        -- д�뼯�й��϶�ĸ澯�������ñ��������������Լ�ͬ���ɹ���omc�б�
                        v_sql:='insert into fm_sync_nms_detail(
                                        report_id,
                                        report_time,
                                        omc_id,
                                        ne_name,
                                        nms_alarm_id,
                                        omc_alarm_id,
                                        omc_alarm_id2,
                                        title,
                                        ne_time,
                                        vendor,
                                        compare_type,
                                        nms_name,
                                        compare_type_jzgz,
                                        event_id
                                )
                                
                                select 
                                        to_char(:v_report_time,''yyyymmddhh24miss'') report_id,
                                        :v_report_time,
                                        e.ems_id omc_id,
                                        e.ne_name,
                                        e.ems_alarm_id nms_alarm_id,
                                        e.omc_alarm_id,
                                        null omc_alarm_id2,
                                        e.title,
                                        e.ne_time,
                                        e.vendor,
                                        null compare_type,
                                        ''���й���'' nms_name,
                                        :x1 compare_type_jzgz,
                                        event_id
                                from fm_alarm e
                                where e.ne_time<=:v_report_time
                                  and e.is_cleared=0
                                  and e.specialty=:x2
                                  and e.title in (select * from table(STR2VARLIST_NEW(:v_titles)))
                                  and e.ems_id in (select * from table(STR2VARLIST_NEW(:v_omc)))
                                  and not exists (select 1 from fm_sync_nms_detail d where d.report_time=:v_report_time and d.nms_alarm_id=e.ems_alarm_id /*and d.ne_time=e.ne_time*/) 
                        ';
                        
                        --��ʶ�澯����һ��                      
                        if cur.device_key is not null and cur.device_value is not null
                        then
                                v_sql:=v_sql || ' and cur.device_key =' || '''cur.device_value''';
                                
                                update fm_sync_nms_detail d set d.synctype=cur.synctype
                                where d.report_time=v_report_time
                                  and d.nms_name in (v_nms_name,'���й���')
                                  and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))
                                  and d.synctype is null
                                  and d.ne_type=cur.device_value
                                ;
                                commit;
                                
                        else
                                update fm_sync_nms_detail d set d.synctype=cur.synctype
                                where d.report_time=v_report_time
                                  and d.nms_name in (v_nms_name,'���й���')
                                  and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))
                                  and d.synctype is null
                                ;
                                commit;

                        end if;
                        
                        
                        -- ��������ͬ���澯��ʱ����ǲ��̶��ģ�������ʱ����ͳ�Ƽ��й��϶�ĸ澯
                        /*
                        if v_nms_name='��������'
                        then
                                execute immediate v_sql using v_report_time,v_report_time,'���й��ϱ�'||v_nms_name||'��',v_report_time,v_specialty,cur.titles,v_omcList,v_report_time;
                        end if;
                        */
                        

                        
                        if v_nms_name in ('��������','��������')
                        then
                                --delete from fm_sync_report d where d.report_time=v_report_time and d.sync_type=cur.synctype;
                                insert into fm_sync_report (report_id,
                                        report_time,
                                        sync_type,
                                        omc_id,
                                        vendor,
                                        network_type,
                                        omc_alarm_total,
                                        nms_alarm_total,
                                        omc2nms_more,
                                        omc2nms_less,
                                        omc2nms_same,
                                        omc2nms_same_rate,
                                        omc2nms_reason,
                                        jzgz_alarm_total,
                                        nms2jzgz_more,
                                        nms2jzgz_less,
                                        nms2jzgz_same,
                                        nms2jzgz_same_rate,
                                        nms2jzgz_reason,
                                        project_alarm)
                                select d.report_id,
                                        d.report_time,
                                        d.synctype,
                                        d.omc_id,
                                        d.vendor,
                                        d.network_type,
                                        sum(case when d.compare_type in('OMC��'||v_nms_name||'��','OMC��'||v_nms_name||'һ��') then 1 else 0 end) omc_alarm_total,
                                        sum(case when d.compare_type in('OMC��'||v_nms_name||'��','OMC��'||v_nms_name||'һ��') then 1 else 0 end) nms_alarm_total,
                                        sum(case when d.compare_type='OMC��'||v_nms_name||'��' then 1 else 0 end ) omc2nms_more,
                                        sum(case when d.compare_type='OMC��'||v_nms_name||'��' then 1 else 0 end ) omc2nms_less,
                                        sum(case when d.compare_type='OMC��'||v_nms_name||'һ��' then 1 else 0 end ) omc2nms_same,
                                        decode(sum(case when d.compare_type in('OMC��'||v_nms_name||'��','OMC��'||v_nms_name||'һ��') then 1 else 0 end),0,100,round(100*sum(case when d.compare_type='OMC��'||v_nms_name||'һ��' then 1 else 0 end )/sum(case when d.compare_type in('OMC��'||v_nms_name||'��','OMC��'||v_nms_name||'һ��') then 1 else 0 end),2)) omc2nms_same_rate,
                                        null omc2nms_reason,
                                        sum(case when d.compare_type_jzgz in('���й��ϱ�'||v_nms_name||'��','���й�����'||v_nms_name||'һ��') and d.compare_type in ('OMC��'||v_nms_name||'һ��','OMC��'||v_nms_name||'��') then 1 else 0 end ) jzgz_alarm_total,
                                        sum(case when d.compare_type_jzgz='���й��ϱ�'||v_nms_name||'��' and d.compare_type in('OMC��'||v_nms_name||'��','OMC��'||v_nms_name||'һ��') then 1 else 0 end ) nms2jzgz_more,
                                        sum(case when d.compare_type_jzgz='���й��ϱ�'||v_nms_name||'��' then 1 else 0 end ) nms2jzgz_less,
                                        sum(case when d.compare_type_jzgz='���й�����'||v_nms_name||'һ��' and d.compare_type in ('OMC��'||v_nms_name||'һ��','OMC��'||v_nms_name||'��') then 1 else 0 end ) nms2jzgz_same,
                                        decode(sum(case when d.compare_type in ('OMC��'||v_nms_name||'һ��','OMC��'||v_nms_name||'��') and d.nms_alarm_id is not null then 1 else 0 end ),0,100,round(100*sum(case when d.compare_type_jzgz='���й�����'||v_nms_name||'һ��' and d.compare_type in ('OMC��'||v_nms_name||'һ��','OMC��'||v_nms_name||'��') then 1 else 0 end )/sum(case when d.compare_type in ('OMC��'||v_nms_name||'һ��','OMC��'||v_nms_name||'��') and d.nms_alarm_id is not null then 1 else 0 end ),2)) nms2jzgz_same_rate,
                                        null nms2jzgz_reason,
                                        sum(case when d.compare_type_jzgz in('���й��ϱ�'||v_nms_name||'��','���й�����'||v_nms_name||'һ��') and d.compare_type in ('OMC��'||v_nms_name||'һ��','OMC��'||v_nms_name||'��') and construct_alarm_flag=1 then 1 else 0 end ) jzgz_alarm_total                                        
                                from fm_sync_nms_detail d 
                                where d.report_time=v_report_time and d.synctype=cur.synctype
                                  and nms_name=v_nms_name
                                  and not exists (select 1 from fm_sync_report d1 where d1.report_time=d.report_time and d1.sync_type=d.synctype and d1.omc_id=d.omc_id)
                                group by d.report_id,d.report_time,d.synctype,d.omc_id,d.vendor,d.network_type;
                                commit;                                
                        end if;
                        
                        if v_nms_name in ('��������')
                        then
                                -- alter by dufs 2018-9-11 ���PON���ܣ��ɼ����Ϊ�յ���������Ӵ����߼�
                                insert into fm_sync_report (report_id,
                                        report_time,
                                        sync_type,
                                        omc_id,
                                        vendor,
                                        omc_alarm_total,
                                        nms_alarm_total,
                                        omc2nms_more,
                                        omc2nms_less,
                                        omc2nms_same,
                                        omc2nms_same_rate,
                                        omc2nms_reason,
                                        jzgz_alarm_total,
                                        nms2jzgz_more,
                                        nms2jzgz_less,
                                        nms2jzgz_same,
                                        nms2jzgz_same_rate,
                                        nms2jzgz_reason
                                        )
                                select distinct d.report_id,
                                        d.report_time,
                                        'OLT��վ' synctype,
                                        d.omc_id,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        100,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        100,
                                        null
                                from fm_sync_nms_detail d 
                                where d.report_time=v_report_time and d.compare_type='������'
                                  and nms_name=v_nms_name
                                  and not exists (select 1 from fm_sync_report d1 where d1.report_time=d.report_time and d1.sync_type='OLT��վ' and d1.omc_id=d.omc_id)  
                                  and exists (select 1 from rm_trans_ems e where d.omc_id=e.objectid_nms and e.remark like 'PON%')    
                                ;
                                commit;
                                
                                
                                update fm_sync_report r set city=(select e.regionname from rm_trans_ems e where r.omc_id=e.objectid_nms and rownum=1) 
                                where r.report_time=v_report_time and r.sync_type=cur.synctype
                                and exists (select e.regionname from rm_trans_ems e where r.omc_id=e.objectid_nms ) 
                                ;
                                commit;
                        
                        end if;
                        
                        --����OMC���ǵ������
                        update fm_sync_report r set r.cover_area=(select e.cover_area from fm_sync_omc e where r.omc_id=e.omc_id and rownum=1) 
                        where r.report_time=v_report_time and r.sync_type=cur.synctype
                        and exists (select e.cover_area from fm_sync_omc e where r.omc_id=e.omc_id )
                        ;
                        commit;                                        
                        
                                                
                        commit;
                end loop;
                
                
                --���¼��й���event_id
                /*
                update fm_sync_nms_detail d set d.event_id=(select d.event_id from fm_alarm e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and rownum=1)
                where d.report_time=v_report_time
                  and d.nms_name =v_nms_name
                  and d.compare_type_jzgz='���й�����'||v_nms_name||'һ��'
                  and d.event_id is null;
                  
                update fm_sync_nms_detail d set d.event_id=(select d.event_id from fm_alarm_history e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and rownum=1)
                where d.report_time=v_report_time
                  and d.nms_name =v_nms_name
                  and d.compare_type_jzgz='���й�����'||v_nms_name||'һ��'
                  and d.event_id is null;                      
                commit;
                */
    end;

BEGIN
        case when v_nms='PN'
        then
                select count(*),max(report_time) into v_cnt,v_report_time 
                from fm_sync_nms_detail_tp_pn tp
                where tp.nms_name='��������'
                  and not exists (select 1 from fm_sync_nms_detail d where tp.report_time=d.report_time and tp.nms_alarm_id=d.nms_alarm_id);

               -- select count(*),max(report_time) into v_cnt,v_report_time from fm_sync_nms_detail_tp tp;

                if v_cnt = 0
                then
                        dbms_output.put_line('not find data.');
                        return;
                elsif v_report_time is null
                then
                        dbms_output.put_line('report_time is null.');
                        return;
                end if;
                
                 --�͵�ȫ���澯��û�������ˣ�����ͨ�������õĸ澯���⣬���˳���Ҫ�ĸ澯
                for cur_title in (select titles from fm_sync_config where specialty in('������','������') order by order_desc)
                loop
                        insert into fm_sync_nms_detail(
                                report_id,
                                report_time,
                                omc_id,
                                ne_name,
                                nms_alarm_id,
                                omc_alarm_id,
                                omc_alarm_id2,
                                title,
                                ne_time,
                                vendor,
                                compare_type,
                                omc_time,
                                ne_type,
                                network_type,
                                nms_name
                        )
                        select 
                                distinct
                                to_char(report_time,'yyyymmddhh24miss') report_id,
                                report_time,
                                omc_id,
                                ne_name,
                                nms_alarm_id,
                                omc_alarm_id,
                                omc_alarm_id2,
                                title,
                                ne_time,
                                m.enum_label vendor,
                                compare_type,
                                omc_time,
                                m2.enum_label ne_type,
                                case when m2.enum_label in ('CELL','BTS','BSC','UTRANCELL','NODEB','RNC') then '2G/3G'
                                     when m2.enum_label in ('EUTRANCELL','ENodeB') then '4G'
                                     when omc_id in ('740','1750') then '������'
                                     else '������'
                                end network_type,
                                nms_name
                        from fm_sync_nms_detail_tp_pn tp
                        left join enum m on m.enum_value=tp.vendor and m.enum_key='FM_COLLECT_VENDOR'
                        left join enum m2 on m2.enum_value=tp.ne_type and m2.enum_key='FM_COLLECT_NETYPE'
                        where exists (select 1 from table(STR2VARLIST_NEW(cur_title.titles)) t where tp.title=t.column_value)
                        and not exists (select 1 from fm_sync_nms_detail d where tp.report_time=d.report_time and tp.nms_alarm_id=d.nms_alarm_id)
                        and not exists (select 1 from fm_sync_nms_detail d where d.report_time=tp.report_time and tp.omc_id=d.omc_id and tp.omc_alarm_id=d.omc_alarm_id)
                        ;
                        commit;
                end loop; 
                
                execute immediate 'truncate table fm_sync_nms_detail_tp_tn';
                         
                FM_SYNC_NMS_COMPARE_PRD(v_report_time,'��������','������');
                FM_SYNC_NMS_COMPARE_PRD(v_report_time,'��������','������');
                
                                
        when v_nms='TN'
        then
                --  ��������ͬ���澯��ʱ����ǲ��̶��ģ�ÿ���һ�Σ����аѱ���ʱ�����������0����
                select count(*),trunc(max(report_time)) into v_cnt,v_report_time 
                from fm_sync_nms_detail_tp_tn tp
                --where tp.nms_name='��������'
                --  and not exists (select 1 from fm_sync_nms_detail d where d.report_time=trunc(tp.report_time) and tp.nms_alarm_id=d.nms_alarm_id)
                  ;
                
                if v_cnt = 0
                then
                        dbms_output.put_line('not find data.');
                        return;
                elsif v_report_time is null
                then
                        dbms_output.put_line('report_time is null.');
                        return;                        
                end if;
                
                --update fm_sync_nms_detail_tp_tn set compare_type='OMC�봫������һ��' where compare_type='����������OMCһ��';
                --update fm_sync_nms_detail_tp_tn set compare_type='OMC�ȴ���������' where compare_type='�������ܱ�OMC��';
                --update fm_sync_nms_detail_tp_tn set compare_type='OMC�ȴ������ܶ�' where compare_type='�������ܱ�OMC��';
                            
                for cur_t in (select report_time from (select trunc(report_time) report_time,count(*) from fm_sync_nms_detail_tp_tn tp group by trunc(report_time) order by 2 desc) where rownum=1
                                --where tp.nms_name='��������'
                               -- and not exists (select 1 from fm_sync_nms_detail d where d.report_time=trunc(tp.report_time) and tp.nms_alarm_id=d.nms_alarm_id)
                )
                loop
                
                        --���������͵�ȫ���澯��û�������ˣ�����ͨ�������õĸ澯���⣬���˳���Ҫ�ĸ澯
                        for cur_title in (select titles from fm_sync_config where specialty='������' order by order_desc)
                        loop
                
                                --  ��������ͬ���澯��ʱ����ǲ��̶��ģ�������ʱ�ѱ���ʱ���������һ������
                                insert into fm_sync_nms_detail(
                                        report_id,
                                        report_time,
                                        omc_id,
                                        ne_name,
                                        nms_alarm_id,
                                        omc_alarm_id,
                                        omc_alarm_id2,
                                        title,
                                        ne_time,
                                        vendor,
                                        compare_type,
                                        nms_name,
                                        ne_type,
                                        omc_time
                                )
                                select 
                                        to_char(trunc(report_time),'yyyymmddhh24miss') report_id,
                                        trunc(report_time) report_time,
                                        omc_id,
                                        ne_name,
                                        nms_alarm_id,
                                        omc_alarm_id,
                                        omc_alarm_id2,
                                        title,
                                        ne_time,
                                        vendor,
                                        case when compare_type='����������OMCһ��' then 'OMC�봫������һ��'
                                             when compare_type='�������ܱ�OMC��'   then 'OMC�ȴ���������'
                                             when compare_type='�������ܱ�OMC��'   then 'OMC�ȴ������ܶ�'
                                             else compare_type
                                        end compare_type,
                                        nms_name,
                                        ne_type,
                                        omc_time
                                from fm_sync_nms_detail_tp_tn tp
                                where trunc(report_time)=cur_t.report_time
                                  and nms_name='��������'
                                  and exists (select 1 from table(STR2VARLIST_NEW(cur_title.titles)) t where tp.title=t.column_value)
                                  and (
                                       not exists (select 1 from fm_sync_nms_detail d where d.report_time=trunc(tp.report_time) and tp.nms_alarm_id=d.nms_alarm_id)
                                       or tp.nms_alarm_id is null and not exists (select 1 from fm_sync_nms_detail d where d.report_time=trunc(tp.report_time) and tp.omc_id=d.omc_id and tp.omc_alarm_id=d.omc_alarm_id)  
                                  )                                
                                  ;
                                commit;
                        
                        end loop;

                        update fm_sync_nms_detail d set (d.city,d.ems_type,d.network_type)=(select e.regionname,e.remark,case when e.remark like 'PON%' then '�ҿ�' else '����' end network_type from rm_trans_ems e where d.omc_id=e.objectid_nms and rownum=1) 
                        where d.report_time=cur_t.report_time and d.nms_name='��������'
                          and exists (select 1 from rm_trans_ems e where d.omc_id=e.objectid_nms ) 
                        ;
                        commit;
        
                        FM_SYNC_NMS_COMPARE_PRD(cur_t.report_time,'��������','������');

                        
                end loop;
                
                execute immediate 'truncate table fm_sync_nms_detail_tp_tn';
                                
        else
                dbms_output.put_line(v_nms||'  is not support.');
        end case;
 
    exception when others then
    v_logInfo := ',SQL-error code:' || sqlcode||sqlerrm;
    v_logInfo := substr(v_logInfo, 0, 1999);
    execute immediate 'insert into prd_exec_log(prd,what,logtype,loginfo) values (:x1,:x2,:x3,:x4)' 
              using v_prd,v_prd,'ERROR',v_loginfo;
    commit;
        
END;
/
