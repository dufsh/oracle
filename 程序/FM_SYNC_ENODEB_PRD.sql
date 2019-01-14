--/
CREATE PROCEDURE FM_SYNC_ENODEB_PRD
(v_report_time  in date default trunc(sysdate,'hh24'))
AS
-- create by dufs 2018-8-10 


-- fm_sync_enodeb_report LTE断站统计差异原因分析报表
-- fm_sync_enodeb_alarm 表记录某个时刻当前活动表中与报表相关的告警信息，后续统计分析都基于这个告警表
-- fm_sync_enodeb_detail 记录各种附表详情


BEGIN

        delete from fm_sync_enodeb_detail where report_time=v_report_time;
        commit;
        
        --更新当前表，
        execute immediate 'truncate table fm_sync_enodeb_alarm';
        
        insert into fm_sync_enodeb_alarm(event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,report_time)
        select event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,v_report_time
        from fm_alarm e 
        where e.is_cleared=0 
          and (e.title in ('网元连接中断','网元断链告警','Heartbeat Failure','NE O&M CONNECTION FAILURE') and e.device_class='ENodeB'
               or e.title in ('[衍生告警]LTE基站断站','[衍生告警]LTE基站断站(未关联到资源)')
               or e.title in ('基站退出服务','BASE STATION FAULTY','4G基站退服告警','eNodeB退服告警')
               or e.title in ('S1ap链路故障') 
              );
        commit;
        
        --写入历史表
        insert into fm_sync_enodeb_alarm_his(event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,report_time)
        select event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,report_time
        from fm_sync_enodeb_alarm;
        commit;
        
        --基站断站数(无线OMC基站脱管+退服告警并剔重)-- 清单
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,first_occurrence_time)
        select to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,'无线OMC基站脱管+退服告警' end ,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,
               e.first_occurrence_time
        from fm_sync_enodeb_alarm e
        where e.title in ('网元连接中断','网元断链告警','Heartbeat Failure','NE O&M CONNECTION FAILURE') and e.device_class='ENodeB'
           or e.title in ('基站退出服务','BASE STATION FAULTY','eNodeB退服告警')
        ;
        commit;
        
        --基站断站数(集中故障系统基站衍生退服告警+基站退服告警+根据小区退服衍生基站退服告警,并剔重)-- 清单
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,first_occurrence_time)
        select to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,'衍生退服告警+基站退服告警' end ,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,
               e.first_occurrence_time
        from fm_sync_enodeb_alarm e
        where e.title in ('基站退出服务','BASE STATION FAULTY','eNodeB退服告警')
           or e.title in ('[衍生告警]LTE基站断站','4G基站退服告警')
        ;
        commit;
        
        -- 附表5、附表10
        -- 爱立信、诺基亚 衍生基站断站告警核查
        -- fm_alarm_relation 表里有关联id相同，但规则不同的情况，需要去重
        --truncate table fm_sync_enodeb_detail;
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,first_occurrence_time)
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,case when t.event_id is not null then '附表10' else '附表5' end ,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,b.s1_ipaddress,b.enodebid,
               t.event_id,t.ne_time,e.first_occurrence_time
        from fm_sync_enodeb_alarm e 
        left join rm_enodeb b on e.ne_id=b.objectid
        left join (select r.relate_event_id,p.ne_time,p.event_id,p.title,p.is_cleared 
                   from fm_alarm_relation r join fm_sync_enodeb_alarm p on p.event_id=r.source_event_id and p.title='[衍生告警]LTE基站断站'
                  ) t on e.event_id=t.relate_event_id
        where e.title in ('网元连接中断','网元断链告警','Heartbeat Failure','NE O&M CONNECTION FAILURE') 
          and e.device_class='ENodeB'
          and e.is_cleared=0 
          and (e.city in ('绵阳','广元','达州','广安','遂宁','巴中','南充','德阳','乐山','攀枝花') or (e.city='凉山' and (e.district is null or e.district!='木里藏族自治县')))
          and e.first_occurrence_time<e.report_time-5/1440
        ;
        commit;
        
        -- 对应未衍生告警的情况，要进一步判断是否抑制情况：已有衍生告警，或者10分钟内有退服告警
        update fm_sync_enodeb_detail d set remark='已衍生[衍生告警]LTE基站断站',d.compare_type='附表10',(derive_event_id,d.derive_ne_time)=(select d1.event_id,d1.ne_time from fm_sync_enodeb_alarm d1 where d.ne_name=d1.ne_name and d1.is_cleared=0 and d1.title='[衍生告警]LTE基站断站')
        where compare_type='附表5' and exists (select 1 from fm_sync_enodeb_alarm d1 where d.ne_name=d1.ne_name and d1.is_cleared=0 and d1.title='[衍生告警]LTE基站断站')
          and d.report_time=v_report_time
        ;
        
        update fm_sync_enodeb_detail d set remark='10分钟内有退服告警',d.compare_type='附表10' where compare_type='附表5' and exists (select 1 from fm_sync_enodeb_alarm d1 where d.ne_name=d1.ne_name and d1.is_cleared=0 and d1.first_occurrence_time>d1.report_time-10/1440 and d1.title in ('基站退出服务','BASE STATION FAULTY','4G基站退服告警','eNodeB退服告警'))
          and d.remark is null
          and d.report_time=v_report_time
        ;
        commit;
        
        
        --附表7，附表9
        -- 华为片区，有脱管告警，没有衍生告警的情况；脱管与衍生一致的情况
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,first_occurrence_time)
        with tab_alm as
        (
                select * from 
                (
                        select e1.*,row_number() over(partition by e1.ne_id order by (case when e1.title='[衍生告警]LTE基站断站' then 1 else 2 end),e1.first_occurrence_time desc) rn
                        from fm_sync_enodeb_alarm e1 where e1.title in ('[衍生告警]LTE基站断站','[衍生告警]LTE基站断站(未关联到资源)') and e1.is_cleared=0
                ) where rn=1
        )
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time, case when t.event_id is null then '附表7' else '附表9' end,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,b.s1_ipaddress,b.enodebid,e.first_occurrence_time
        from fm_sync_enodeb_alarm e 
        left join rm_enodeb b on e.ne_id=b.objectid
        left join tab_alm t on e.ne_id=t.ne_id
        where e.title in ('网元连接中断','网元断链告警','Heartbeat Failure','NE O&M CONNECTION FAILURE') 
          and e.device_class='ENodeB'
          and e.is_cleared=0
          and (e.city not in ('绵阳','广元','达州','广安','遂宁','巴中','南充','德阳','乐山','攀枝花','凉山') or e.city='凉山' and e.district='木里藏族自治县')
          and e.first_occurrence_time<e.report_time-5/1440
        ;
        commit;
        
        
        --附表8
        --华为片区，有S1链路衍生告警,但无脱管告警的基站
        
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,first_occurrence_time)
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time, '附表8' end,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,b.s1_ipaddress,b.enodebid,e.first_occurrence_time
        from fm_sync_enodeb_alarm e 
        left join rm_enodeb b on e.ne_id=b.objectid
        where e.title in ('[衍生告警]LTE基站断站','[衍生告警]LTE基站断站(未关联到资源)') and e.is_cleared=0
          and (e.city not in ('绵阳','广元','达州','广安','遂宁','巴中','南充','德阳','乐山','攀枝花','凉山') or e.city='凉山' and e.district='木里藏族自治县')
          and not exists (select 1 from fm_sync_enodeb_alarm e1 where e1.title in ('网元连接中断','网元断链告警','Heartbeat Failure','NE O&M CONNECTION FAILURE') 
                          and e1.device_class='ENodeB' and e1.is_cleared=0 and e.ne_id=e1.ne_id
                          )
        ;
        commit;        
        
        -- 附表4、2 该衍生实际未衍生的S1断站告警列表、资源问题-资源信息不正确的ENODEBID与基站IP清单
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,first_occurrence_time)
        select distinct to_char(t.report_time,'yyyymmddhh24miss'),t.report_time,
               case when b.objectid is not null then '附表4' else '附表2' end ,
               t.city,t.ne_name,t.title,t.event_id,t.ems_alarm_id,t.omc_alarm_id,t.ne_time,
               b.s1_ipaddress,
               nvl(b.enodebid,t.enodebid),
               first_occurrence_time
        from (
                select e.*,
                       regexp_replace(replace(replace(regexp_substr(e.summary,'移动国家码=[0-9]+, 移动网号=[0-9]+, EnodeB类型=([a-z]|[A-Z]|[0-9])+, EnodeB编码=[0-9]+'),'移动国家码=',''),', 移动网号=','-'),', EnodeB类型=([a-z]|[A-Z]|[0-9])+, EnodeB编码=','-') enodebid,
                       count(distinct e.ne_name) over(partition by e.resource_key) bts_alm,
                       mme_cnt
                from fm_sync_enodeb_alarm e 
                join (select mmepool_name,mme_name,count(*) over(partition by mmepool_name) mme_cnt from fm_mme_pool p where MME_VENDOR = '华为'
                     ) p on e.ne_name = p.mme_name
                where e.title in ('S1ap链路故障') 
                and e.first_occurrence_time<e.report_time-5/1440
        ) t
        left join rm_enodeb b on t.enodebid=b.enodebid
        where mme_cnt=bts_alm
          and not exists (select 1 from fm_sync_enodeb_alarm a where a.title in ('[衍生告警]LTE基站断站') and a.ne_id=t.resource_key)
          and not exists (select 1 from fm_sync_enodeb_alarm a where a.title in ('基站退出服务','BASE STATION FAULTY','4G基站退服告警','eNodeB退服告警') and a.ne_id=t.resource_key and a.first_occurrence_time>a.report_time-10/1440)
        ;
        commit;
        
        --附表3，副表3:资源问题-不能匹配资源的基站清单
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,first_occurrence_time)
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,'附表3',
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,
               e.first_occurrence_time
        from fm_sync_enodeb_alarm e
        where (e.title in ('网元连接中断','网元断链告警','Heartbeat Failure','NE O&M CONNECTION FAILURE') and e.device_class='ENodeB'
        or e.title in ('基站退出服务','BASE STATION FAULTY','eNodeB退服告警'))
        --and not exists(select 1 from rm_enodeb b where e.ne_id=b.objectid)
        and city is null
        ;
        commit;
        
        
        
        
        -- alter by dufs 2018-9-18 按客户韩文刚要求，将 集中故障少统计的原因: 未分类，归入“附表6”
        -- 未分类的详情写入表中

        -- omc多的告警清单
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time)
        with tab_omc_more as
        (
                select d.*
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.compare_type='无线OMC基站脱管+退服告警'
                and d.report_time=v_report_time
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d1.compare_type='衍生退服告警+基站退服告警' and d1.report_time=v_report_time and d.ne_name=d1.ne_name and nvl(d.city,'不能区分地市')=nvl(d1.city,'不能区分地市'))
        )
        --集中故障少统计的原因: 未分类
        select report_id,report_time,'附表6' compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time
        from tab_omc_more t
        where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('附表2','附表3') and d.report_time=v_report_time and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=nvl(d.city,'不能区分地市'))
          and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('附表4') and t.ne_name=b.objectname and nvl(t.city,'不能区分地市')=nvl(b.regionname,'不能区分地市'))          
          and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('附表5') and d.report_time=v_report_time and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=nvl(d.city,'不能区分地市'))
          and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('附表6','附表7') and d.report_time=v_report_time and  t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=nvl(d.city,'不能区分地市'))
        ;
        commit;
        
        -- 更新备注
        update fm_sync_enodeb_detail d set d.remark='5分钟内上报告警，未到衍生时间' where d.report_time=v_report_time and compare_type='附表6' and d.first_occurrence_time>d.into_time-5/1440;
        commit;
        
        
        -- alter by dufs 2018-9-18 按客户韩文刚要求，将 集中故障多统计的原因: 未分类，归入 “附件8”

        -- 集中故障多的告警清单
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time)
        --集中故障多的告警清单
        with tab_zhjk_more as
        (
                select d.*,b.enodebid,b.s1_ipaddress
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.compare_type='衍生退服告警+基站退服告警'
                and d.report_time=v_report_time
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d1.compare_type='无线OMC基站脱管+退服告警' and d1.report_time=v_report_time and d.ne_name=d1.ne_name and nvl(d.city,'不能区分地市')=nvl(d1.city,'不能区分地市'))
        )
        --集中故障多统计的原因: 未分类
        select report_id,report_time,'附表8' compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,s1_ipaddress enodeb_ip,enodebid enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time
        from tab_zhjk_more t
        where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('附表8') and d.report_time=v_report_time and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=nvl(d.city,'不能区分地市'))
        ;
        commit;
        
         -- 更新备注
        update fm_sync_enodeb_detail d set d.remark='话务网管衍生' where d.report_time=v_report_time and compare_type='附表8' and d.title ='4G基站退服告警';
        commit;
        
        delete from fm_sync_enodeb_report where report_time=v_report_time;
        
        --地市和mme_pool划分
        insert into fm_sync_enodeb_report(
                report_id,
                report_time,
                city,
                is_huawei_mmepool,
                omc_alarm_total,
                jzgz_alarm_total,
                jzgz2nms_less_reason1,
                jzgz2nms_less_reason2,
                jzgz2nms_less_reason3,
                --jzgz2nms_less_reason,
                jzgz2nms_more_reason1,
                --jzgz2nms_more_reason,
                jzgz2nms_same
        )
        with tab_rg as
        (
                select column_value city,'否' is_huawei_mmepool from table(str2varlist('绵阳,广元,达州,广安,遂宁,巴中,南充,德阳,乐山,攀枝花,凉山'))
                union all
                select objectname city,'是' is_huawei_mmepool from rm_region where objecttype='地市' and objectname not in (select column_value from table(str2varlist('绵阳,广元,达州,广安,遂宁,巴中,南充,德阳,乐山,攀枝花,凉山')))
                union all
                select '凉山木里' city,'是' is_huawei_mmepool from dual
                union all
                select '不能区分地市' city,'否' is_huawei_mmepool from dual
        ),
        --omc纬度统计的告警数量
        tab_omc_cnt as
        (
                select case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end city,
                count(distinct d.ne_name) cnt
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='无线OMC基站脱管+退服告警'
                group by case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end
        ),
        --集中故障纬度统计的告警数量
        tab_zhjk_cnt as
        (
                select case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end city,
                count(distinct d.ne_name) cnt
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='衍生退服告警+基站退服告警'
                group by case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end
        ),
        -- omc多的告警清单
        tab_omc_more as
        (
                select distinct case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end city,d.ne_name
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='无线OMC基站脱管+退服告警'
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d.report_time=d1.report_time and d1.compare_type='衍生退服告警+基站退服告警' and d.ne_name=d1.ne_name and nvl(d.city,'不能区分地市')=nvl(d1.city,'不能区分地市'))
        ),
        --集中故障多的告警清单
        tab_zhjk_more as
        (
                select distinct case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end city,d.ne_name
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='衍生退服告警+基站退服告警'
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d.report_time=d1.report_time and d1.compare_type='无线OMC基站脱管+退服告警' and d.ne_name=d1.ne_name and nvl(d.city,'不能区分地市')=nvl(d1.city,'不能区分地市'))
        ),
        --一致的告警数量
        tab_same_cnt as 
        (
                select case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end city,count(distinct d.ne_name) cnt
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='衍生退服告警+基站退服告警'
                and exists (select 1 from fm_sync_enodeb_detail d1 where d.report_time=d1.report_time and d1.compare_type='无线OMC基站脱管+退服告警' and d.ne_name=d1.ne_name and nvl(d.city,'不能区分地市')=nvl(d1.city,'不能区分地市'))
                group by case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end
        ),
        --A: 集中故障少统计的原因: 资源不一致
        tab_jzgz_less1_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表2','附表3') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(d.city,'不能区分地市') end))
                group by t.city
        ),
        --B :集中故障少统计的原因: 集中故障功能问题
        tab_jzgz_less2_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表2','附表3') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(d.city,'不能区分地市') end))
                  and (exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('附表4') and t.ne_name=b.objectname and nvl(t.city,'不能区分地市')=nvl(b.regionname,'不能区分地市'))
                       or exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表5') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(d.city,'不能区分地市') end))
                       )
                group by t.city
        ),
        --C: 集中故障少统计的原因: 无线侧统计为断站,但集中故障未统计数量
        tab_jzgz_less3_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表2','附表3') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(d.city,'不能区分地市') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('附表4') and t.ne_name=b.objectname and nvl(t.city,'不能区分地市')=nvl(b.regionname,'不能区分地市'))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表5') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(d.city,'不能区分地市') end))
                 -- and exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表6','附表7') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end))
                group by t.city
        ),
        /*
        --集中故障少统计的原因: 未分类
        tab_jzgz_less_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表2','附表3') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('附表4') and t.ne_name=b.objectname and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表5') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表6','附表7') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end))
                group by t.city
        ),
        */
        --D:集中故障多统计的原因:集中故障统计为断站,但无线侧未统计数量
        tab_jzgz_more1_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_zhjk_more t
               -- where exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表8') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end))
                group by t.city
        )/*,
        
        --集中故障多统计的原因:未分类
        tab_jzgz_more_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_zhjk_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('附表8') and t.ne_name=d.ne_name and nvl(t.city,'不能区分地市')=(case when d.city='凉山' and b.countyname='木里藏族自治县' then '凉山木里' else nvl(city,'不能区分地市') end))
                group by t.city
        )
        */
        select to_char(v_report_time,'yyyymmddhh24miss'),
               v_report_time,       
               rg.city,
               rg.is_huawei_mmepool,
               nvl(omc.cnt,0) omc_alarm_total,
               nvl(zhjk.cnt,0) jzgz_alarm_total,
               nvl(less1.cnt,0) jzgz2nms_less_reason1,
               nvl(less2.cnt,0) jzgz2nms_less_reason2,
               nvl(less3.cnt,0) jzgz2nms_less_reason3,
               --nvl(less.cnt,0) jzgz2nms_less_reason,
               nvl(more1.cnt,0) jzgz2nms_more_reason1,
               --nvl(more.cnt,0) jzgz2nms_more_reason,
               nvl(same.cnt,0) jzgz2nms_same
        from tab_rg rg
        left join tab_omc_cnt omc on rg.city=omc.city
        left join tab_zhjk_cnt zhjk on rg.city=zhjk.city
        left join tab_same_cnt same on rg.city=same.city
        left join tab_jzgz_less1_cnt less1 on rg.city=less1.city
        left join tab_jzgz_less2_cnt less2 on rg.city=less2.city
        left join tab_jzgz_less3_cnt less3 on rg.city=less3.city
        --left join tab_jzgz_less_cnt less on rg.city=less.city
        left join tab_jzgz_more1_cnt more1 on rg.city=more1.city
        --left join tab_jzgz_more_cnt more on rg.city=more.city
        ;
        commit;
        
        delete from fm_sync_enodeb_alarm_his where report_time<trunc(sysdate)-30;
        commit;
        
END;
/
