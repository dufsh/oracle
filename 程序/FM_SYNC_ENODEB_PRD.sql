--/
CREATE PROCEDURE FM_SYNC_ENODEB_PRD
(v_report_time  in date default trunc(sysdate,'hh24'))
AS
-- create by dufs 2018-8-10 


-- fm_sync_enodeb_report LTE��վͳ�Ʋ���ԭ���������
-- fm_sync_enodeb_alarm ���¼ĳ��ʱ�̵�ǰ������뱨����صĸ澯��Ϣ������ͳ�Ʒ�������������澯��
-- fm_sync_enodeb_detail ��¼���ָ�������


BEGIN

        delete from fm_sync_enodeb_detail where report_time=v_report_time;
        commit;
        
        --���µ�ǰ��
        execute immediate 'truncate table fm_sync_enodeb_alarm';
        
        insert into fm_sync_enodeb_alarm(event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,report_time)
        select event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,v_report_time
        from fm_alarm e 
        where e.is_cleared=0 
          and (e.title in ('��Ԫ�����ж�','��Ԫ�����澯','Heartbeat Failure','NE O&M CONNECTION FAILURE') and e.device_class='ENodeB'
               or e.title in ('[�����澯]LTE��վ��վ','[�����澯]LTE��վ��վ(δ��������Դ)')
               or e.title in ('��վ�˳�����','BASE STATION FAULTY','4G��վ�˷��澯','eNodeB�˷��澯')
               or e.title in ('S1ap��·����') 
              );
        commit;
        
        --д����ʷ��
        insert into fm_sync_enodeb_alarm_his(event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,report_time)
        select event_id,severity,resource_id,resource_key,resource_name,resource_type,device_class,device_category,specialty,network_type,vendor,title,subject,first_occurrence_time,last_occurrence_time,occurrence_times,summary,is_derivative,is_parent,is_acknowledged,ack_user,ack_time,is_cleared,clear_time,clear_user,clear_reason,ems_alarm_id,ems_clear_id,original_severity,vendor_alarm_code,locate_info,ems_time,ne_time,ems_clear_time,ne_clear_time,finish_time,eoms_send_state,eoms_send_type,eoms_send_time,eoms_sheet_id,city,district,site,construct_alarm_flag,project_id,system_alarm_code,event_type,event_class,event_sub_class,affect_device,affect_business,correlate_flag,alarm_standard_name,alarm_explanation,send_group_flag,transport_rate,ems_id,ne_id,ne_name,ne_alias,ne_state,ne_ip,card_id,card_name,port_id,port_name,circuit_id,circuit,service_id,customer_id,additional_info1,additional_info2,additional_info3,additional_time,affect_user_number,abnormal,correlate_rule_name,eoms_rule_id,eoms_should_send_time,local_severity,self_define_title,vip_flag,topo_graphy_flag,trxnumber,business,network,sectionname,vendor_severity,affect_ne_number,original_locate_info,probable_cause,system_name,card_model,vendor_version,omc_alarm_id,layer_rate,alarm_checkpoint,additional_info_std,resource_type_std,rate_relation,time_slot,pre_deal_info,rlocateint,rlocatesdn,rlocatenename,rlocatenetype,device_class_std,vendor_std,specialty_std,severity_std,property,report_time
        from fm_sync_enodeb_alarm;
        commit;
        
        --��վ��վ��(����OMC��վ�ѹ�+�˷��澯������)-- �嵥
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,first_occurrence_time)
        select to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,'����OMC��վ�ѹ�+�˷��澯' end ,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,
               e.first_occurrence_time
        from fm_sync_enodeb_alarm e
        where e.title in ('��Ԫ�����ж�','��Ԫ�����澯','Heartbeat Failure','NE O&M CONNECTION FAILURE') and e.device_class='ENodeB'
           or e.title in ('��վ�˳�����','BASE STATION FAULTY','eNodeB�˷��澯')
        ;
        commit;
        
        --��վ��վ��(���й���ϵͳ��վ�����˷��澯+��վ�˷��澯+����С���˷�������վ�˷��澯,������)-- �嵥
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,first_occurrence_time)
        select to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,'�����˷��澯+��վ�˷��澯' end ,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,
               e.first_occurrence_time
        from fm_sync_enodeb_alarm e
        where e.title in ('��վ�˳�����','BASE STATION FAULTY','eNodeB�˷��澯')
           or e.title in ('[�����澯]LTE��վ��վ','4G��վ�˷��澯')
        ;
        commit;
        
        -- ����5������10
        -- �����š�ŵ���� ������վ��վ�澯�˲�
        -- fm_alarm_relation �����й���id��ͬ��������ͬ���������Ҫȥ��
        --truncate table fm_sync_enodeb_detail;
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,first_occurrence_time)
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,case when t.event_id is not null then '����10' else '����5' end ,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,b.s1_ipaddress,b.enodebid,
               t.event_id,t.ne_time,e.first_occurrence_time
        from fm_sync_enodeb_alarm e 
        left join rm_enodeb b on e.ne_id=b.objectid
        left join (select r.relate_event_id,p.ne_time,p.event_id,p.title,p.is_cleared 
                   from fm_alarm_relation r join fm_sync_enodeb_alarm p on p.event_id=r.source_event_id and p.title='[�����澯]LTE��վ��վ'
                  ) t on e.event_id=t.relate_event_id
        where e.title in ('��Ԫ�����ж�','��Ԫ�����澯','Heartbeat Failure','NE O&M CONNECTION FAILURE') 
          and e.device_class='ENodeB'
          and e.is_cleared=0 
          and (e.city in ('����','��Ԫ','����','�㰲','����','����','�ϳ�','����','��ɽ','��֦��') or (e.city='��ɽ' and (e.district is null or e.district!='ľ�����������')))
          and e.first_occurrence_time<e.report_time-5/1440
        ;
        commit;
        
        -- ��Ӧδ�����澯�������Ҫ��һ���ж��Ƿ�������������������澯������10���������˷��澯
        update fm_sync_enodeb_detail d set remark='������[�����澯]LTE��վ��վ',d.compare_type='����10',(derive_event_id,d.derive_ne_time)=(select d1.event_id,d1.ne_time from fm_sync_enodeb_alarm d1 where d.ne_name=d1.ne_name and d1.is_cleared=0 and d1.title='[�����澯]LTE��վ��վ')
        where compare_type='����5' and exists (select 1 from fm_sync_enodeb_alarm d1 where d.ne_name=d1.ne_name and d1.is_cleared=0 and d1.title='[�����澯]LTE��վ��վ')
          and d.report_time=v_report_time
        ;
        
        update fm_sync_enodeb_detail d set remark='10���������˷��澯',d.compare_type='����10' where compare_type='����5' and exists (select 1 from fm_sync_enodeb_alarm d1 where d.ne_name=d1.ne_name and d1.is_cleared=0 and d1.first_occurrence_time>d1.report_time-10/1440 and d1.title in ('��վ�˳�����','BASE STATION FAULTY','4G��վ�˷��澯','eNodeB�˷��澯'))
          and d.remark is null
          and d.report_time=v_report_time
        ;
        commit;
        
        
        --����7������9
        -- ��ΪƬ�������ѹܸ澯��û�������澯��������ѹ�������һ�µ����
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,first_occurrence_time)
        with tab_alm as
        (
                select * from 
                (
                        select e1.*,row_number() over(partition by e1.ne_id order by (case when e1.title='[�����澯]LTE��վ��վ' then 1 else 2 end),e1.first_occurrence_time desc) rn
                        from fm_sync_enodeb_alarm e1 where e1.title in ('[�����澯]LTE��վ��վ','[�����澯]LTE��վ��վ(δ��������Դ)') and e1.is_cleared=0
                ) where rn=1
        )
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time, case when t.event_id is null then '����7' else '����9' end,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,b.s1_ipaddress,b.enodebid,e.first_occurrence_time
        from fm_sync_enodeb_alarm e 
        left join rm_enodeb b on e.ne_id=b.objectid
        left join tab_alm t on e.ne_id=t.ne_id
        where e.title in ('��Ԫ�����ж�','��Ԫ�����澯','Heartbeat Failure','NE O&M CONNECTION FAILURE') 
          and e.device_class='ENodeB'
          and e.is_cleared=0
          and (e.city not in ('����','��Ԫ','����','�㰲','����','����','�ϳ�','����','��ɽ','��֦��','��ɽ') or e.city='��ɽ' and e.district='ľ�����������')
          and e.first_occurrence_time<e.report_time-5/1440
        ;
        commit;
        
        
        --����8
        --��ΪƬ������S1��·�����澯,�����ѹܸ澯�Ļ�վ
        
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,first_occurrence_time)
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time, '����8' end,
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,b.s1_ipaddress,b.enodebid,e.first_occurrence_time
        from fm_sync_enodeb_alarm e 
        left join rm_enodeb b on e.ne_id=b.objectid
        where e.title in ('[�����澯]LTE��վ��վ','[�����澯]LTE��վ��վ(δ��������Դ)') and e.is_cleared=0
          and (e.city not in ('����','��Ԫ','����','�㰲','����','����','�ϳ�','����','��ɽ','��֦��','��ɽ') or e.city='��ɽ' and e.district='ľ�����������')
          and not exists (select 1 from fm_sync_enodeb_alarm e1 where e1.title in ('��Ԫ�����ж�','��Ԫ�����澯','Heartbeat Failure','NE O&M CONNECTION FAILURE') 
                          and e1.device_class='ENodeB' and e1.is_cleared=0 and e.ne_id=e1.ne_id
                          )
        ;
        commit;        
        
        -- ����4��2 ������ʵ��δ������S1��վ�澯�б���Դ����-��Դ��Ϣ����ȷ��ENODEBID���վIP�嵥
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,first_occurrence_time)
        select distinct to_char(t.report_time,'yyyymmddhh24miss'),t.report_time,
               case when b.objectid is not null then '����4' else '����2' end ,
               t.city,t.ne_name,t.title,t.event_id,t.ems_alarm_id,t.omc_alarm_id,t.ne_time,
               b.s1_ipaddress,
               nvl(b.enodebid,t.enodebid),
               first_occurrence_time
        from (
                select e.*,
                       regexp_replace(replace(replace(regexp_substr(e.summary,'�ƶ�������=[0-9]+, �ƶ�����=[0-9]+, EnodeB����=([a-z]|[A-Z]|[0-9])+, EnodeB����=[0-9]+'),'�ƶ�������=',''),', �ƶ�����=','-'),', EnodeB����=([a-z]|[A-Z]|[0-9])+, EnodeB����=','-') enodebid,
                       count(distinct e.ne_name) over(partition by e.resource_key) bts_alm,
                       mme_cnt
                from fm_sync_enodeb_alarm e 
                join (select mmepool_name,mme_name,count(*) over(partition by mmepool_name) mme_cnt from fm_mme_pool p where MME_VENDOR = '��Ϊ'
                     ) p on e.ne_name = p.mme_name
                where e.title in ('S1ap��·����') 
                and e.first_occurrence_time<e.report_time-5/1440
        ) t
        left join rm_enodeb b on t.enodebid=b.enodebid
        where mme_cnt=bts_alm
          and not exists (select 1 from fm_sync_enodeb_alarm a where a.title in ('[�����澯]LTE��վ��վ') and a.ne_id=t.resource_key)
          and not exists (select 1 from fm_sync_enodeb_alarm a where a.title in ('��վ�˳�����','BASE STATION FAULTY','4G��վ�˷��澯','eNodeB�˷��澯') and a.ne_id=t.resource_key and a.first_occurrence_time>a.report_time-10/1440)
        ;
        commit;
        
        --����3������3:��Դ����-����ƥ����Դ�Ļ�վ�嵥
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,first_occurrence_time)
        select distinct to_char(e.report_time,'yyyymmddhh24miss'),e.report_time,'����3',
               e.city,e.ne_name,e.title,e.event_id,e.ems_alarm_id,e.omc_alarm_id,e.ne_time,
               e.first_occurrence_time
        from fm_sync_enodeb_alarm e
        where (e.title in ('��Ԫ�����ж�','��Ԫ�����澯','Heartbeat Failure','NE O&M CONNECTION FAILURE') and e.device_class='ENodeB'
        or e.title in ('��վ�˳�����','BASE STATION FAULTY','eNodeB�˷��澯'))
        --and not exists(select 1 from rm_enodeb b where e.ne_id=b.objectid)
        and city is null
        ;
        commit;
        
        
        
        
        -- alter by dufs 2018-9-18 ���ͻ����ĸ�Ҫ�󣬽� ���й�����ͳ�Ƶ�ԭ��: δ���࣬���롰����6��
        -- δ���������д�����

        -- omc��ĸ澯�嵥
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time)
        with tab_omc_more as
        (
                select d.*
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.compare_type='����OMC��վ�ѹ�+�˷��澯'
                and d.report_time=v_report_time
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d1.compare_type='�����˷��澯+��վ�˷��澯' and d1.report_time=v_report_time and d.ne_name=d1.ne_name and nvl(d.city,'�������ֵ���')=nvl(d1.city,'�������ֵ���'))
        )
        --���й�����ͳ�Ƶ�ԭ��: δ����
        select report_id,report_time,'����6' compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time
        from tab_omc_more t
        where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('����2','����3') and d.report_time=v_report_time and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=nvl(d.city,'�������ֵ���'))
          and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('����4') and t.ne_name=b.objectname and nvl(t.city,'�������ֵ���')=nvl(b.regionname,'�������ֵ���'))          
          and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('����5') and d.report_time=v_report_time and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=nvl(d.city,'�������ֵ���'))
          and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('����6','����7') and d.report_time=v_report_time and  t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=nvl(d.city,'�������ֵ���'))
        ;
        commit;
        
        -- ���±�ע
        update fm_sync_enodeb_detail d set d.remark='5�������ϱ��澯��δ������ʱ��' where d.report_time=v_report_time and compare_type='����6' and d.first_occurrence_time>d.into_time-5/1440;
        commit;
        
        
        -- alter by dufs 2018-9-18 ���ͻ����ĸ�Ҫ�󣬽� ���й��϶�ͳ�Ƶ�ԭ��: δ���࣬���� ������8��

        -- ���й��϶�ĸ澯�嵥
        insert into fm_sync_enodeb_detail(report_id,report_time,compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,enodeb_ip,enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time)
        --���й��϶�ĸ澯�嵥
        with tab_zhjk_more as
        (
                select d.*,b.enodebid,b.s1_ipaddress
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.compare_type='�����˷��澯+��վ�˷��澯'
                and d.report_time=v_report_time
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d1.compare_type='����OMC��վ�ѹ�+�˷��澯' and d1.report_time=v_report_time and d.ne_name=d1.ne_name and nvl(d.city,'�������ֵ���')=nvl(d1.city,'�������ֵ���'))
        )
        --���й��϶�ͳ�Ƶ�ԭ��: δ����
        select report_id,report_time,'����8' compare_type,city,ne_name,title,event_id,nms_alarm_id,omc_alarm_id,ne_time,s1_ipaddress enodeb_ip,enodebid enodeb_id,derive_event_id,derive_ne_time,into_time,remark,first_occurrence_time
        from tab_zhjk_more t
        where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.compare_type in ('����8') and d.report_time=v_report_time and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=nvl(d.city,'�������ֵ���'))
        ;
        commit;
        
         -- ���±�ע
        update fm_sync_enodeb_detail d set d.remark='������������' where d.report_time=v_report_time and compare_type='����8' and d.title ='4G��վ�˷��澯';
        commit;
        
        delete from fm_sync_enodeb_report where report_time=v_report_time;
        
        --���к�mme_pool����
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
                select column_value city,'��' is_huawei_mmepool from table(str2varlist('����,��Ԫ,����,�㰲,����,����,�ϳ�,����,��ɽ,��֦��,��ɽ'))
                union all
                select objectname city,'��' is_huawei_mmepool from rm_region where objecttype='����' and objectname not in (select column_value from table(str2varlist('����,��Ԫ,����,�㰲,����,����,�ϳ�,����,��ɽ,��֦��,��ɽ')))
                union all
                select '��ɽľ��' city,'��' is_huawei_mmepool from dual
                union all
                select '�������ֵ���' city,'��' is_huawei_mmepool from dual
        ),
        --omcγ��ͳ�Ƶĸ澯����
        tab_omc_cnt as
        (
                select case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end city,
                count(distinct d.ne_name) cnt
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='����OMC��վ�ѹ�+�˷��澯'
                group by case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end
        ),
        --���й���γ��ͳ�Ƶĸ澯����
        tab_zhjk_cnt as
        (
                select case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end city,
                count(distinct d.ne_name) cnt
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='�����˷��澯+��վ�˷��澯'
                group by case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end
        ),
        -- omc��ĸ澯�嵥
        tab_omc_more as
        (
                select distinct case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end city,d.ne_name
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='����OMC��վ�ѹ�+�˷��澯'
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d.report_time=d1.report_time and d1.compare_type='�����˷��澯+��վ�˷��澯' and d.ne_name=d1.ne_name and nvl(d.city,'�������ֵ���')=nvl(d1.city,'�������ֵ���'))
        ),
        --���й��϶�ĸ澯�嵥
        tab_zhjk_more as
        (
                select distinct case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end city,d.ne_name
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='�����˷��澯+��վ�˷��澯'
                and not exists (select 1 from fm_sync_enodeb_detail d1 where d.report_time=d1.report_time and d1.compare_type='����OMC��վ�ѹ�+�˷��澯' and d.ne_name=d1.ne_name and nvl(d.city,'�������ֵ���')=nvl(d1.city,'�������ֵ���'))
        ),
        --һ�µĸ澯����
        tab_same_cnt as 
        (
                select case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end city,count(distinct d.ne_name) cnt
                from fm_sync_enodeb_detail d
                left join rm_enodeb b on d.ne_name=b.objectname
                where d.report_time=v_report_time
                and d.compare_type='�����˷��澯+��վ�˷��澯'
                and exists (select 1 from fm_sync_enodeb_detail d1 where d.report_time=d1.report_time and d1.compare_type='����OMC��վ�ѹ�+�˷��澯' and d.ne_name=d1.ne_name and nvl(d.city,'�������ֵ���')=nvl(d1.city,'�������ֵ���'))
                group by case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end
        ),
        --A: ���й�����ͳ�Ƶ�ԭ��: ��Դ��һ��
        tab_jzgz_less1_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����2','����3') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(d.city,'�������ֵ���') end))
                group by t.city
        ),
        --B :���й�����ͳ�Ƶ�ԭ��: ���й��Ϲ�������
        tab_jzgz_less2_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����2','����3') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(d.city,'�������ֵ���') end))
                  and (exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('����4') and t.ne_name=b.objectname and nvl(t.city,'�������ֵ���')=nvl(b.regionname,'�������ֵ���'))
                       or exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����5') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(d.city,'�������ֵ���') end))
                       )
                group by t.city
        ),
        --C: ���й�����ͳ�Ƶ�ԭ��: ���߲�ͳ��Ϊ��վ,�����й���δͳ������
        tab_jzgz_less3_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����2','����3') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(d.city,'�������ֵ���') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('����4') and t.ne_name=b.objectname and nvl(t.city,'�������ֵ���')=nvl(b.regionname,'�������ֵ���'))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����5') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(d.city,'�������ֵ���') end))
                 -- and exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����6','����7') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end))
                group by t.city
        ),
        /*
        --���й�����ͳ�Ƶ�ԭ��: δ����
        tab_jzgz_less_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_omc_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����2','����3') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.enodeb_id=b.enodebid where d.report_time=v_report_time and d.compare_type in ('����4') and t.ne_name=b.objectname and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����5') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end))
                  and not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����6','����7') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end))
                group by t.city
        ),
        */
        --D:���й��϶�ͳ�Ƶ�ԭ��:���й���ͳ��Ϊ��վ,�����߲�δͳ������
        tab_jzgz_more1_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_zhjk_more t
               -- where exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����8') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end))
                group by t.city
        )/*,
        
        --���й��϶�ͳ�Ƶ�ԭ��:δ����
        tab_jzgz_more_cnt as
        (
                select t.city,count(distinct t.ne_name) cnt
                from tab_zhjk_more t
                where not exists (select 1 from fm_sync_enodeb_detail d left join rm_enodeb b on d.ne_name=b.objectname where d.report_time=v_report_time and d.compare_type in ('����8') and t.ne_name=d.ne_name and nvl(t.city,'�������ֵ���')=(case when d.city='��ɽ' and b.countyname='ľ�����������' then '��ɽľ��' else nvl(city,'�������ֵ���') end))
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
