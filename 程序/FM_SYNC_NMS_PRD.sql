--/
CREATE PROCEDURE FM_SYNC_NMS_PRD
(v_nms  in varchar2)
AS

-- create by dufs 2018-8-22 
-- 调用方式：211.4服务器上脚本调用
/*
#=============== create by dufs 2018-8-20 接入专业网管与OMC告警对比详情，需求负责人：薛宇 ======================
10 * * * * . $HOME/.profile;cd /backup/fm_sync; ./fm_sync_pn_Run.sh
10 * * * * . $HOME/.profile;cd /backup/fm_sync; ./fm_sync_tn_Run.sh
#=============== create by dufs 2018-8-20 接入专业网管与OMC告警对比详情，需求负责人：薛宇 ======================
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
                -- 得到omc列表，在这些omc范围内比较告警，原因：专业网管存在部分omc告警同步失败等原因，如果不限制omc范围，则会出现集中故障明显会多很多告警的情况
                select listagg(omc_id,',') within group(order by omc_id) into v_omcList
                from (
                        select distinct d.omc_id
                        from fm_sync_nms_detail d
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                     );
                
                --按配置表中需要比对的告警项，逐项与集中故障告警进行比较         
                for cur in (select synctype,titles,device_key,device_value from fm_sync_config where specialty=v_specialty order by order_desc)
                loop
                        dbms_output.put_line(cur.synctype);
                        -- 标识一致的告警，活动库中对比
                        update fm_sync_nms_detail d set d.compare_type_jzgz='集中故障与'||v_nms_name||'一致',
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 and rownum=1)
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 /*and e.ne_time=d.ne_time*/)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;
                        commit;
                        
                        update fm_sync_nms_detail d set d.compare_type_jzgz='集中故障与'||v_nms_name||'一致',
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm_permanent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 and rownum=1) 
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm_permanent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 /*and e.ne_time=d.ne_time*/)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;
                        commit;  
                        
                        update fm_sync_nms_detail d set d.compare_type_jzgz='集中故障与'||v_nms_name||'一致', 
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm_recent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 and rownum=1) 
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm_recent e where e.ems_alarm_id=d.nms_alarm_id and e.is_derivative=0 /*and e.ne_time=d.ne_time*/)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;
                        commit;                        

                        -- 标识一致的告警，历史库中对比
                        update fm_sync_nms_detail d set d.compare_type_jzgz='集中故障与'||v_nms_name||'一致',
                                                       (d.nms_time,d.jzgz_time,d.event_id,d.construct_alarm_flag,d.city)=(select e.ems_time,e.first_occurrence_time,e.event_id,e.construct_alarm_flag,e.city from fm_alarm_history e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and e.is_derivative=0 and rownum=1)  
                        where d.report_time=v_report_time
                          and d.nms_name=v_nms_name
                          and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))                     
                          and exists (select 1 from fm_alarm_history e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and e.is_derivative=0)
                          and d.nms_alarm_id is not null
                          and d.compare_type_jzgz is null
                        ;                        
                        commit;

                        -- 标识集中故障少的告警   筛选nms_alarm_id不为空的数据，发现有标识为：OMC与专业网管一致，但专业网管流水号为空的情况
                        update fm_sync_nms_detail d set d.compare_type_jzgz='集中故障比'||v_nms_name||'少' 
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

                        -- 写入集中故障多的告警，按配置表中设置条件，以及同步成功的omc列表
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
                                        ''集中故障'' nms_name,
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
                        
                        --标识告警是哪一组                      
                        if cur.device_key is not null and cur.device_value is not null
                        then
                                v_sql:=v_sql || ' and cur.device_key =' || '''cur.device_value''';
                                
                                update fm_sync_nms_detail d set d.synctype=cur.synctype
                                where d.report_time=v_report_time
                                  and d.nms_name in (v_nms_name,'集中故障')
                                  and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))
                                  and d.synctype is null
                                  and d.ne_type=cur.device_value
                                ;
                                commit;
                                
                        else
                                update fm_sync_nms_detail d set d.synctype=cur.synctype
                                where d.report_time=v_report_time
                                  and d.nms_name in (v_nms_name,'集中故障')
                                  and d.title in (select * from table(STR2VARLIST_NEW(cur.titles)))
                                  and d.synctype is null
                                ;
                                commit;

                        end if;
                        
                        
                        -- 传输网管同步告警的时间点是不固定的，所有暂时不能统计集中故障多的告警
                        /*
                        if v_nms_name='话务网管'
                        then
                                execute immediate v_sql using v_report_time,v_report_time,'集中故障比'||v_nms_name||'多',v_report_time,v_specialty,cur.titles,v_omcList,v_report_time;
                        end if;
                        */
                        

                        
                        if v_nms_name in ('话务网管','传输网管')
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
                                        sum(case when d.compare_type in('OMC比'||v_nms_name||'多','OMC与'||v_nms_name||'一致') then 1 else 0 end) omc_alarm_total,
                                        sum(case when d.compare_type in('OMC比'||v_nms_name||'少','OMC与'||v_nms_name||'一致') then 1 else 0 end) nms_alarm_total,
                                        sum(case when d.compare_type='OMC比'||v_nms_name||'多' then 1 else 0 end ) omc2nms_more,
                                        sum(case when d.compare_type='OMC比'||v_nms_name||'少' then 1 else 0 end ) omc2nms_less,
                                        sum(case when d.compare_type='OMC与'||v_nms_name||'一致' then 1 else 0 end ) omc2nms_same,
                                        decode(sum(case when d.compare_type in('OMC比'||v_nms_name||'多','OMC与'||v_nms_name||'一致') then 1 else 0 end),0,100,round(100*sum(case when d.compare_type='OMC与'||v_nms_name||'一致' then 1 else 0 end )/sum(case when d.compare_type in('OMC比'||v_nms_name||'多','OMC与'||v_nms_name||'一致') then 1 else 0 end),2)) omc2nms_same_rate,
                                        null omc2nms_reason,
                                        sum(case when d.compare_type_jzgz in('集中故障比'||v_nms_name||'多','集中故障与'||v_nms_name||'一致') and d.compare_type in ('OMC与'||v_nms_name||'一致','OMC比'||v_nms_name||'少') then 1 else 0 end ) jzgz_alarm_total,
                                        sum(case when d.compare_type_jzgz='集中故障比'||v_nms_name||'少' and d.compare_type in('OMC比'||v_nms_name||'少','OMC与'||v_nms_name||'一致') then 1 else 0 end ) nms2jzgz_more,
                                        sum(case when d.compare_type_jzgz='集中故障比'||v_nms_name||'多' then 1 else 0 end ) nms2jzgz_less,
                                        sum(case when d.compare_type_jzgz='集中故障与'||v_nms_name||'一致' and d.compare_type in ('OMC与'||v_nms_name||'一致','OMC比'||v_nms_name||'少') then 1 else 0 end ) nms2jzgz_same,
                                        decode(sum(case when d.compare_type in ('OMC与'||v_nms_name||'一致','OMC比'||v_nms_name||'少') and d.nms_alarm_id is not null then 1 else 0 end ),0,100,round(100*sum(case when d.compare_type_jzgz='集中故障与'||v_nms_name||'一致' and d.compare_type in ('OMC与'||v_nms_name||'一致','OMC比'||v_nms_name||'少') then 1 else 0 end )/sum(case when d.compare_type in ('OMC与'||v_nms_name||'一致','OMC比'||v_nms_name||'少') and d.nms_alarm_id is not null then 1 else 0 end ),2)) nms2jzgz_same_rate,
                                        null nms2jzgz_reason,
                                        sum(case when d.compare_type_jzgz in('集中故障比'||v_nms_name||'多','集中故障与'||v_nms_name||'一致') and d.compare_type in ('OMC与'||v_nms_name||'一致','OMC比'||v_nms_name||'少') and construct_alarm_flag=1 then 1 else 0 end ) jzgz_alarm_total                                        
                                from fm_sync_nms_detail d 
                                where d.report_time=v_report_time and d.synctype=cur.synctype
                                  and nms_name=v_nms_name
                                  and not exists (select 1 from fm_sync_report d1 where d1.report_time=d.report_time and d1.sync_type=d.synctype and d1.omc_id=d.omc_id)
                                group by d.report_id,d.report_time,d.synctype,d.omc_id,d.vendor,d.network_type;
                                commit;                                
                        end if;
                        
                        if v_nms_name in ('传输网管')
                        then
                                -- alter by dufs 2018-9-11 针对PON网管，采集结果为空的情况，增加处理逻辑
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
                                        'OLT断站' synctype,
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
                                where d.report_time=v_report_time and d.compare_type='空数据'
                                  and nms_name=v_nms_name
                                  and not exists (select 1 from fm_sync_report d1 where d1.report_time=d.report_time and d1.sync_type='OLT断站' and d1.omc_id=d.omc_id)  
                                  and exists (select 1 from rm_trans_ems e where d.omc_id=e.objectid_nms and e.remark like 'PON%')    
                                ;
                                commit;
                                
                                
                                update fm_sync_report r set city=(select e.regionname from rm_trans_ems e where r.omc_id=e.objectid_nms and rownum=1) 
                                where r.report_time=v_report_time and r.sync_type=cur.synctype
                                and exists (select e.regionname from rm_trans_ems e where r.omc_id=e.objectid_nms ) 
                                ;
                                commit;
                        
                        end if;
                        
                        --更新OMC覆盖地市情况
                        update fm_sync_report r set r.cover_area=(select e.cover_area from fm_sync_omc e where r.omc_id=e.omc_id and rownum=1) 
                        where r.report_time=v_report_time and r.sync_type=cur.synctype
                        and exists (select e.cover_area from fm_sync_omc e where r.omc_id=e.omc_id )
                        ;
                        commit;                                        
                        
                                                
                        commit;
                end loop;
                
                
                --更新集中故障event_id
                /*
                update fm_sync_nms_detail d set d.event_id=(select d.event_id from fm_alarm e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and rownum=1)
                where d.report_time=v_report_time
                  and d.nms_name =v_nms_name
                  and d.compare_type_jzgz='集中故障与'||v_nms_name||'一致'
                  and d.event_id is null;
                  
                update fm_sync_nms_detail d set d.event_id=(select d.event_id from fm_alarm_history e where e.ems_alarm_id=d.nms_alarm_id and e.ne_time=d.ne_time and rownum=1)
                where d.report_time=v_report_time
                  and d.nms_name =v_nms_name
                  and d.compare_type_jzgz='集中故障与'||v_nms_name||'一致'
                  and d.event_id is null;                      
                commit;
                */
    end;

BEGIN
        case when v_nms='PN'
        then
                select count(*),max(report_time) into v_cnt,v_report_time 
                from fm_sync_nms_detail_tp_pn tp
                where tp.nms_name='话务网管'
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
                
                 --送的全量告警，没有做过滤，这里通过已配置的告警标题，过滤出需要的告警
                for cur_title in (select titles from fm_sync_config where specialty in('话务网','动环网') order by order_desc)
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
                                     when omc_id in ('740','1750') then '动环网'
                                     else '核心网'
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
                         
                FM_SYNC_NMS_COMPARE_PRD(v_report_time,'话务网管','话务网');
                FM_SYNC_NMS_COMPARE_PRD(v_report_time,'话务网管','动环网');
                
                                
        when v_nms='TN'
        then
                --  传输网管同步告警的时间点是不固定的，每天出一次，所有把报表时间归整到当天0点整
                select count(*),trunc(max(report_time)) into v_cnt,v_report_time 
                from fm_sync_nms_detail_tp_tn tp
                --where tp.nms_name='传输网管'
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
                
                --update fm_sync_nms_detail_tp_tn set compare_type='OMC与传输网管一致' where compare_type='传输网管与OMC一致';
                --update fm_sync_nms_detail_tp_tn set compare_type='OMC比传输网管少' where compare_type='传输网管比OMC多';
                --update fm_sync_nms_detail_tp_tn set compare_type='OMC比传输网管多' where compare_type='传输网管比OMC少';
                            
                for cur_t in (select report_time from (select trunc(report_time) report_time,count(*) from fm_sync_nms_detail_tp_tn tp group by trunc(report_time) order by 2 desc) where rownum=1
                                --where tp.nms_name='传输网管'
                               -- and not exists (select 1 from fm_sync_nms_detail d where d.report_time=trunc(tp.report_time) and tp.nms_alarm_id=d.nms_alarm_id)
                )
                loop
                
                        --传输网管送的全量告警，没有做过滤，这里通过已配置的告警标题，过滤出需要的告警
                        for cur_title in (select titles from fm_sync_config where specialty='传输网' order by order_desc)
                        loop
                
                                --  传输网管同步告警的时间点是不固定的，所有暂时把报表时间归整到下一个整点
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
                                        case when compare_type='传输网管与OMC一致' then 'OMC与传输网管一致'
                                             when compare_type='传输网管比OMC多'   then 'OMC比传输网管少'
                                             when compare_type='传输网管比OMC少'   then 'OMC比传输网管多'
                                             else compare_type
                                        end compare_type,
                                        nms_name,
                                        ne_type,
                                        omc_time
                                from fm_sync_nms_detail_tp_tn tp
                                where trunc(report_time)=cur_t.report_time
                                  and nms_name='传输网管'
                                  and exists (select 1 from table(STR2VARLIST_NEW(cur_title.titles)) t where tp.title=t.column_value)
                                  and (
                                       not exists (select 1 from fm_sync_nms_detail d where d.report_time=trunc(tp.report_time) and tp.nms_alarm_id=d.nms_alarm_id)
                                       or tp.nms_alarm_id is null and not exists (select 1 from fm_sync_nms_detail d where d.report_time=trunc(tp.report_time) and tp.omc_id=d.omc_id and tp.omc_alarm_id=d.omc_alarm_id)  
                                  )                                
                                  ;
                                commit;
                        
                        end loop;

                        update fm_sync_nms_detail d set (d.city,d.ems_type,d.network_type)=(select e.regionname,e.remark,case when e.remark like 'PON%' then '家宽' else '传输' end network_type from rm_trans_ems e where d.omc_id=e.objectid_nms and rownum=1) 
                        where d.report_time=cur_t.report_time and d.nms_name='传输网管'
                          and exists (select 1 from rm_trans_ems e where d.omc_id=e.objectid_nms ) 
                        ;
                        commit;
        
                        FM_SYNC_NMS_COMPARE_PRD(cur_t.report_time,'传输网管','传输网');

                        
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
