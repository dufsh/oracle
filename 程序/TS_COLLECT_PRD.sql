--/
CREATE PROCEDURE TS_COLLECT_PRD( TYPE in varchar2)
AS

-- add by dufs 2017-05-05
-- �ۺ������ϳ��� ���ݴ���
 
-- ���÷�ʽ 211.4��������crontab ����
-- #=========================== �ۺ������ϳ������ݽ��� =========================================================
-- 3,18,33,48 * * * * . $HOME/.profile; cd /backup/ts_collect; ./ts_city_info_15_Run.sh >/dev/null 2>&1
-- #=========================== �ۺ������ϳ������ݽ��� =========================================================

v_createtime date;
v_table varchar2(64);
v_sql varchar2(2000);
v_logInfo varchar2(500);

begin
        
        case when TYPE='CITY'
        then
                select max(createtime) into v_createtime from ts_city_info_15_tp ;
                if v_createtime is null
                then
                      return;
                end if;
                
             --   insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(  insert  into ts_city_info_15)','��ʼִ��');

             --15���ӵ���ҵ��ۺϱ�
                insert /* + append */ into ts_city_info_15 (createtime,cityid,city,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)
                select createtime,cityid,m.enum_value,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500
                from ts_city_info_15_tp t
                left join ts_enum m on t.cityid=m.enum_key and m.enum_type='����' 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��' ;
                --where not exists (select 1 from ts_city_info_15 a1 where a1.createtime=t.createtime and a1.cityid=t.cityid and a1.at=t.at and a1.ast=t.ast);
                commit;
                
              --  insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(  insert  into ts_city_info_15)','ִ�н���');
              --  commit;
                
                TS_CITY_COUNT_PRD('AST');
              --  insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_CITY_COUNT_PRD - AST )','ִ�н���');
              --  commit;
                
                TS_CITY_COUNT_PRD('AT');
             --   insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_CITY_COUNT_PRD - AT )','ִ�н���');
             --   commit;

                /*  �����±��۵�ʱ�䡢�����������ȼ�¼��ts_dataimport_record�У�����ǰ̨չʾʹ��
                ts_city_info_15
                ts_prov_info_15
                ts_city_apptype_info_15
                ts_prov_apptype_info_15
                */
               for cur_cell in (select relatetable from ts_enum_table_relation where basetable ='ts_city_info_15_tp'
                 )loop
                 
                        v_table:=cur_cell.relatetable;
                        v_sql:='insert into ts_dataimport_record(createtime,table_name,cnt) select * from (select :x1 as createtime,:x2 as table_name,count(*) as cnt from '||v_table||' t where createtime=:x1 ) t1 where not exists (select 1 from ts_dataimport_record t2 where t1.createtime=t2.createtime and t2.table_name=:x2)';
                        execute immediate v_sql using v_createtime,v_table,v_createtime,v_table;
                        commit;
                       -- DBms_output.put_line(v_table||'----'||v_createtime||'-----'||v_sql);
               end loop;
               
              -- insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_city_info_15_tp )','ִ�н���');
              -- commit;

               TS_ALARM_PRD_NEW('PROV_ACC_SUCC');
               
               -- add by dufs 2018-5-29
               -- ʡ������ҵ����ࡢҵ��С��ָ���ҵ����ʳɹ��ʡ����������������쳣�澯��������ȥ����ߡ���ͺ�ƽ��ֵ�½�30%��
               TS_ALARM_PRD_NEW('PROV_AT_APP_SUCC');
               TS_ALARM_PRD_NEW('PROV_AT_APP_DL');
               TS_ALARM_PRD_NEW('PROV_AT_APP_RATE');
               TS_ALARM_PRD_NEW('PROV_AST_APP_SUCC');
               TS_ALARM_PRD_NEW('PROV_AST_APP_DL');
               TS_ALARM_PRD_NEW('PROV_AST_APP_RATE');
               TS_ALARM_PRD_NEW('CITY_AT_APP_SUCC');
               TS_ALARM_PRD_NEW('CITY_AT_APP_DL');
               TS_ALARM_PRD_NEW('CITY_AT_APP_RATE');
               TS_ALARM_PRD_NEW('CITY_AST_APP_SUCC');
               TS_ALARM_PRD_NEW('CITY_AST_APP_DL');
               TS_ALARM_PRD_NEW('CITY_AST_APP_RATE');

        when TYPE='CELL_PART'
        then
                
                -- �ж�������ļ������ļ��������ݣ�����tp���У�����С���ͳ�����д��ts_cell_info_15
                insert into ts_cell_info_15_tp (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)                
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012')
                ;
                commit;
                
                /*
                union
                -- 2017-10-26 ���Ӿ�����С�����ݣ�Ϊ������ؼ�������ָ��׼��
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��'
                where exists (select 1 from rm_eutrancell eu where eu.objectid=r.objectid and eu.scencetype in ('�Ͳ������','����Ⱥ','�߲������','���д�'))
                ;
                commit;
                
                */
                
                insert /* + append */ into ts_cell_info_15 (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)                
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012')
                ;
                commit;
                
                /*
                -- �ж�������ļ������ļ��������ݣ�����tp���У�����С���ͳ�����д��ts_cell_info_15
                insert into ts_cell_info_15 (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)                
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                join rm_eutrancell_eci_n r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012')
                 or r.scencetype in ('�Ͳ������','����Ⱥ','�߲������','���д�')
                union
                -- 2017-10-26 ���Ӿ�����С�����ݣ�Ϊ������ؼ�������ָ��׼��
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��'
                where exists (select 1 from rm_eutrancell eu where eu.objectid=r.objectid and eu.scencetype in ('�Ͳ������','����Ⱥ','�߲������','���д�'))
                ;
                */
                
                commit;

        when TYPE='CELL'
        then
                select max(createtime) into v_createtime from ts_cell_info_15_tp;
                if v_createtime is null
                then
                      return;
                end if;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_info_15  )_3_1','��ʼִ��');

             --15����С��ҵ���С��
             /*
                insert into ts_cell_info_15 (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2)
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2
                from ts_cell_info_15_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��' ;
              --  where not exists (select 1 from ts_cell_info_15 a1 where a1.createtime=t.createtime and a1.eci=t.eci and a1.at=t.at and a1.ast=t.ast) ;
              */

                -- dufs 2018-1-24 �����ʱ��Ҫִ�н���5���ӣ� �ĳ���CELL_PART�����д��ķ�ʽ��30����
                --insert into ts_cell_info_15 select * from ts_cell_info_15_tp;
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_info_15  )_3_2','ִ�н���');
                commit;

                TS_AREA_COUNT_PRD('AST');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_AREA_COUNT_PRD - AST )_3_3','ִ�н���');
                commit;

                TS_SEN_COUNT_PRD('AST');      
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_SEN_COUNT_PRD - AST )_3_4','ִ�н���');
                commit;
                
                --ָ��ҵ��С�࣬�����ػ������
                TS_COUNTY_COUNT_PRD('AST');

                /*�����±��۵�ʱ�䡢�����������ȼ�¼��ts_dataimport_record�У�����ǰ̨չʾʹ��
                ts_area_count
                ts_sen_count
                */
                 for cur_cell in (select relatetable from ts_enum_table_relation where basetable ='ts_cell_info_15_tp'
                 )loop
                 
                        v_table:=cur_cell.relatetable;
                        v_sql:='insert into ts_dataimport_record(createtime,table_name,cnt) select * from (select :x1 as createtime,:x2 as table_name,count(*) as cnt from '||v_table||' t where createtime=:x1 ) t1 where not exists (select 1 from ts_dataimport_record t2 where t1.createtime=t2.createtime and t2.table_name=:x2)';
                        execute immediate v_sql using v_createtime,v_table,v_createtime,v_table;
                        commit;
                       -- DBms_output.put_line(v_table||'----'||v_createtime||'-----'||v_sql);
               end loop;
               
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_cell_info_15_tp )_3_5','ִ�н���');
               commit;
               
       
        when TYPE='CELL_DZG'
        then
                -- ҵ����ࡱ���С���ҵ��С�ࡱ���ƹ񡰣�ÿСʱ1�����ݣ���Ҫ�Զ���������ÿ15����1������
                select max(createtime) into v_createtime from ts_cell_info_15_tp_dzg;
                if v_createtime is null
                then
                      return;
                end if;
                
                execute immediate 'truncate table ts_cell_info_15_dzg';
                
                insert /* + append */ into ts_cell_info_15_dzg (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)                
                select t_day,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_dzg t
                left join (select v_createtime + (level-1 ) / 96 t_day from dual connect by level <= 4) a on 1=1
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012');
                commit;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_info_15_dzg  )','ִ�н���');
                commit;

                TS_AREA_COUNT_PRD('DZG');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_AREA_COUNT_PRD - DZG )','ִ�н���');
                commit;

                TS_SEN_COUNT_PRD('DZG');      
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_SEN_COUNT_PRD - DZG )','ִ�н���');
                commit;  
                             
        when TYPE='CELL_APPTYPE_PART'
        then
                -- �ж�������ļ������ļ��������ݣ�����tp���У�����С���ͳ�����д��ts_cell_apptype_info_15
                insert into ts_cell_apptype_info_15_tp (createtime,eci,cell_id,cell_name,at,at_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)
                select createtime,t.eci,r.objectid,r.objectname,at,m1.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500
                from ts_cell_apptype_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m1 on t.at=m1.enum_key and m1.enum_type='ҵ�����'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012');
                commit;
              
        when TYPE='CELL_APPTYPE'
        then
                select max(createtime) into v_createtime from ts_cell_apptype_info_15_tp;
                
                if v_createtime is null
                then
                      return;
                end if;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_apptype_info_15  )_1_1','��ʼִ��');
                commit;

             --15����С��ҵ�����
             --2017-5-28 ֻ�����볡����ص�С������
             /*
                insert into ts_cell_apptype_info_15 (createtime,eci,cell_id,cell_name,at,at_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)
                select createtime,t.eci,r.objectid,r.objectname,at,m1.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500
                from ts_cell_apptype_info_15_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m1 on t.at=m1.enum_key and m1.enum_type='ҵ�����'
                where exists (select 1 from tscene_sre sre join tscene_sba sba on sre.be_id=sba.area_id and sba.sen_type_id='ST0012' join tscene_areainfo area on sba.area_id=area.area_id where sre.object_id=r.objectid);
                commit;
                */
                insert /* + append */ into ts_cell_apptype_info_15 select * from ts_cell_apptype_info_15_tp;
                commit;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_apptype_info_15  )_1_2','ִ�н���');
                commit;

                TS_AREA_COUNT_PRD('AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_AREA_COUNT_PRD - AT )_1_3','ִ�н���');
                commit;
                
                TS_SEN_COUNT_PRD('AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_SEN_COUNT_PRD - AT )_1_4','ִ�н���');
                commit;

                --С��ҵ�����ָ��Ԥ������
                TS_RULE_WARN_PRD('CELL_AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - CELL_AT )_1_5','ִ�н���');
                commit;

                --����ҵ�����ָ��Ԥ������
                TS_RULE_WARN_PRD('AREA_AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - AREA_AT )_1_6','ִ�н���');
               commit;   

                 /*�����±��۵�ʱ�䡢�����������ȼ�¼��ts_dataimport_record�У�����ǰ̨չʾʹ��
                ts_area_apptype_count
                ts_sen_apptype_count
                */
                 for cur_cell in (select relatetable from ts_enum_table_relation where basetable ='ts_cell_apptype_info_15_tp'
                 )loop
                 
                        v_table:=cur_cell.relatetable;
                        v_sql:='insert into ts_dataimport_record(createtime,table_name,cnt) select * from (select :x1 as createtime,:x2 as table_name,count(*) as cnt from '||v_table||' t where createtime=:x1 ) t1 where not exists (select 1 from ts_dataimport_record t2 where t1.createtime=t2.createtime and t2.table_name=:x2)';
                        execute immediate v_sql using v_createtime,v_table,v_createtime,v_table;
                        commit;
                       -- DBms_output.put_line(v_table||'----'||v_createtime||'-----'||v_sql);
               end loop;
               
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_cell_apptype_info_15 )_1_7','ִ�н���');
               commit;   
               
               -- ����С��Ԥ�����������澯
               -- TS_ALARM_PRD();
               --TS_ALARM_PRD_NEW('AREA'); --��5�������滻
               --insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(AREA) )_1_8','ִ�н���');
               --TS_ALARM_PRD_NEW('CITY'); --��5�������滻
               --insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(CITY) )_1_9','ִ�н���');               
               
               --TS_ALARM_PRD_NEW('CELL'); --��5�������滻
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(CELL) )_1_10','ִ�н���');  
               TS_ALARM_PRD_NEW('CELL_5PKG');
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(CELL_5PKG) )_1_11','ִ�н���'); 
               
               --add by dufs-2018-2-8 ���Ҫ��TS_ALARM_PRD_NEW('CELL_5PKG')�Ļ�����ִ��
               -- С��5�������ۼƴﵽ1/3
               TS_ALARM_PRD_NEW('CELL_AREA_5PKG');
               -- С��5�������ۼƴ�200�λ�1/10С��
               TS_ALARM_PRD_NEW('CELL_CITY_5PKG'); 
               -- С��5�������ۼƴﵽ20��
               TS_ALARM_PRD_NEW('CELL_5PKG_TOTAL'); 
               
               -- ����ʱ�ж���ts_dataimport_record�����������ts_cell_common_info_15���¼�¼������Ҫ�ŵ�������
               TS_ALARM_PRD_NEW('CELL_OVERFLOW');
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_ALARM_PRD_NEW - CELL_OVERFLOW )_2_11','ִ�н���');
               commit;   
                               
               -- ��¼����ʱ��
               v_table:='ts_alarm';
               v_sql:='insert into ts_dataimport_record(createtime,table_name,cnt) select * from (select :x1 as createtime,:x2 as table_name,count(*) as cnt from '||v_table||' t where createtime=:x1 ) t1 where not exists (select 1 from ts_dataimport_record t2 where t1.createtime=t2.createtime and t1.table_name=t2.table_name)';
               execute immediate v_sql using v_createtime,v_table,v_createtime;
               commit;     
                    
        when TYPE='CELL_COMMON'
        then
                select max(createtime) into v_createtime from ts_cell_common_info_15_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_common_info_15_tp )_2_1','��ʼִ��');                
                commit;
                
             --15����С��ͨ��ָ��
                insert /* + append */ into ts_cell_common_info_15 (createtime,eci,cell_id,cell_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,delta_ty_dl,delta_ty_rate,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)
                select createtime,t.eci,r.objectid,r.objectname,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,delta_ty_dl,delta_ty_rate,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500
                from ts_cell_common_info_15_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012')
                   or exists (select 1 from ts_cell_vip v where r.eci=v.eci)
                   or exists (select 1 from rm_enodeb b join rm_eutrancell r1 on r1.enodebid=b.objectid where r.objectid=r1.objectid and b.isvvipbts=1)
                   ;
                commit;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_common_info_15_tp )_2_2','ִ�н���');                
                commit;                
                
                -- �Ȼ���м���ģ�Ȼ��ѳ������С����tp����ɾ������ߺ������ڸñ�Ĳ���Ч�ʡ�
                TS_CITY_COUNT_PRD('COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_CITY_COUNT_PRD - COMMON )_2_3','ִ�н���');
                commit; 
                
                /*
                delete from ts_cell_common_info_15_tp t 
                where not exists (select 1 from tscene_sre sre join tscene_sba sba on sre.be_id=sba.area_id and sba.sen_type_id='ST0012' join tscene_areainfo area on sba.area_id=area.area_id join rm_eutrancell_eci r1 on sre.object_id=r1.objectid 
                                  where r1.eci=t.eci);
                commit;
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( delete from ts_cell_common_info_15_tp t not exists)_2_4','ִ�н���');                
                */
                TS_AREA_COUNT_PRD('COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_AREA_COUNT_PRD - COMMON )_2_5','ִ�н���');                
                commit;   
                
                TS_SEN_COUNT_PRD('COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_SEN_COUNT_PRD - COMMON )_2_6','ִ�н���');                
                commit;   



                --С��ͨ��ָ��Ԥ������
                TS_RULE_WARN_PRD('CELL_COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - CELL_COMMON )_2_7','ִ�н���');
                commit;      
                
                -- add by dufs 2017-11-9 ֻ���˫ʮһ�����ڼ䣬���ɾ�����С����ͨ��Ԥ��
                if (sysdate >=to_date('2017-11-09','yyyy-mm-dd') and sysdate<to_date('2017-11-12','yyyy-mm-dd'))
                then 
                        TS_RULE_WARN_PRD('CELL_COMMON_JMQ');
                        insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - CELL_COMMON_JMQ )_2_8','ִ�н���');
                        commit;
                end if;
                
                 --����ͨ��ָ��Ԥ������
                TS_RULE_WARN_PRD('AREA_COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - AREA_COMMON )_2_9','ִ�н���');
                commit;   
                                               
                 /*�����±��۵�ʱ�䡢�����������ȼ�¼��ts_dataimport_record�У�����ǰ̨չʾʹ��
                ts_area_common_count
                ts_sen_common_count
                */
                
                 for cur_cell in (select relatetable from ts_enum_table_relation where basetable ='ts_cell_common_info_15_tp'
                 )loop
                 
                        v_table:=cur_cell.relatetable;
                        v_sql:='insert into ts_dataimport_record(createtime,table_name,cnt) select * from (select :x1 as createtime,:x2 as table_name,count(*) as cnt from '||v_table||' t where createtime=:x1 ) t1 where not exists (select 1 from ts_dataimport_record t2 where t1.createtime=t2.createtime and t2.table_name=:x2)';
                        execute immediate v_sql using v_createtime,v_table,v_createtime,v_table;
                        commit;
                       -- DBms_output.put_line(v_table||'----'||v_createtime||'-----'||v_sql);
               end loop;               

               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_cell_common_info_15_tp )_2_10','ִ�н���');
               commit;   
 
                -- ����Ĳ�����Ҫ�ж�ts_dataimport_record�������¼�¼��ts_cell_common_info_15�����Ϣ�����Է�����Ҫ�ŵ����
                -- ͨ��ҵ��HTTP���ʳɹ��ʵ���һ������ VVIPС��
                TS_ALARM_PRD_NEW('CELL_APP_SUCC');
                -- ͨ��ҵ��HTTPƽ���������ʵ���һ������ VVIPС��
                TS_ALARM_PRD_NEW('CELL_HTTP_DL_RATE');
                -- ͨ�����ش�����������澯
                TS_ALARM_PRD_NEW('CELL_COUNTY_FLOW_LOW');
                -- ͨ�õ��д�����������澯
                TS_ALARM_PRD_NEW('CELL_CITY_FLOW_LOW');
                
                              

        -- �����С��������ÿСʱ��۵��û���
        when TYPE='cityScene'
        then
                select max(createtime) into v_createtime from ts_cityscene_usercount_tp;
                if v_createtime is null
                then
                      return;
                end if;
                
                insert /* + append */ into ts_cityscene_usercount (createtime,cityid,city,sen_id,sen_name,user_count,granularity,object_type)
                select t.createtime,t.cityid,m.enum_value,t.sen_id,s.sen_name,user_count,granularity,
                case when t.cityid=0 and t.sen_id='8888' then 'ʡ'
                     when t.cityid=0 and t.sen_id is not null and t.sen_id!='8888' then 'ʡ+����'
                     when t.cityid!=0 and t.sen_id='8888' then '����'
                     when t.cityid!=0 and t.sen_id is not null and t.sen_id!='8888' then '����+����'
                end object_type
                from ts_cityscene_usercount_tp t
                left join ts_enum m on t.cityid=m.enum_key and m.enum_type='����'
                left join tscene_seninfo s on s.sen_id=t.sen_id
                ;
                commit;

        -- �����С�������������ÿСʱ��۵��û���
        when TYPE='sceneArea'
        then
                select max(createtime) into v_createtime from ts_scenearea_usercount_tp;
                if v_createtime is null
                then
                      return;
                end if;
                
                insert /* + append */ into ts_scenearea_usercount (createtime,cityid,city,sen_id,sen_name,area_id,area_name,user_count,granularity)
                select t.createtime,t.cityid,m.enum_value,t.sen_id,s.sen_name,t.area_id,a.area_name,user_count,granularity
                from ts_scenearea_usercount_tp t
                left join ts_enum m on t.cityid=m.enum_key and m.enum_type='����'
                left join tscene_seninfo s on s.sen_id=t.sen_id
                left join tscene_areainfo a on a.area_id=t.area_id
                ;
                commit;                

        -- ÿ��С���澯����
        when TYPE='CELL_ALARM'
        then
                select max(createtime) into v_createtime from ts_cell_alarm_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_alarm_tp )','��ʼִ��');                
                commit;

                insert into ts_cell_alarm (createtime,city,eci,cell_name,cell_id,at,ast,at_name,ast_name,alarm_count,is_alarm,alarm_title,alarm_rule,alarm_level,alarm_info,statnum)
                select t.createtime,r.regionname,t.eci,t.cell_name,r.objectid,t.at,t.ast,t.at_name,t.ast_name,t.alarm_count,t.is_alarm,t.alarm_title,t.alarm_rule,t.alarm_level,t.alarm_info,statnum
                from ts_cell_alarm_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci ;
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_alarm_tp )','ִ�н���');                
                commit;
                
        -- ÿ��С������澯����
        when TYPE='CELL_CLEAR'
        then
                select max(createtime) into v_createtime from ts_cell_clear_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_cell_clear_tp )','��ʼִ��');                
                commit;
                
                --����ECI��at_code��alarm_title��ƥ��7�����ڵĸ澯��Ϣ����ƥ���ϵĸ澯���Ƿ������is_clear����Ϣ����Ϊ1
                update ts_cell_alarm t set is_clear=0,clear_time=(select max(createtime) from ts_cell_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title)
                where t.createtime>=trunc(v_createtime)-7 and t.createtime<trunc(v_createtime)
                and exists (select 1 from ts_cell_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title);
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_cell_clear_tp )','ִ�н���');                
                commit;  
                

        -- ÿ������澯����
        when TYPE='AREA_ALARM'
        then
                select max(createtime) into v_createtime from ts_area_alarm_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_area_alarm_tp )','��ʼִ��');                
                commit;

                insert into ts_area_alarm (createtime,city,eci,cell_name,cell_id,area_id,area_name,at,ast,at_name,ast_name,alarm_count,is_alarm,alarm_title,alarm_rule,alarm_level,alarm_info)
                select t.createtime,r.regionname,t.eci,t.cell_name,r.objectid,t.area_id,t.area_name,t.at,t.ast,t.at_name,t.ast_name,t.alarm_count,t.is_alarm,t.alarm_title,t.alarm_rule,t.alarm_level,t.alarm_info
                from ts_area_alarm_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci ;
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_area_alarm_tp )','ִ�н���');                
                commit;
                
        -- ÿ����������澯����
        when TYPE='AREA_CLEAR'
        then
                select max(createtime) into v_createtime from ts_area_clear_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_area_clear_tp )','��ʼִ��');                
                commit;
                
                --����ECI��at_code��alarm_title��ƥ��7�����ڵĸ澯��Ϣ����ƥ���ϵĸ澯���Ƿ������is_clear����Ϣ����Ϊ1
                update ts_area_alarm t set is_clear=0,clear_time=(select max(createtime) from ts_area_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title)
                where t.createtime>=trunc(v_createtime)-7 and t.createtime<trunc(v_createtime)
                and exists (select 1 from ts_area_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title);
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_area_clear_tp )','ִ�н���');                
                commit;  

        when TYPE='COUNTY'
        then
                select max(createtime) into v_createtime from ts_county_info_15_tp ;
                if v_createtime is null
                then
                      return;
                end if;
                
             --15��������ҵ��ۺϱ�
                insert /* + append */ into ts_county_info_15 (createtime,countyid,county,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2)
                select createtime,countyid,m.enum_value,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2
                from ts_county_info_15_tp t
                left join ts_enum m on t.countyid=m.enum_key and m.enum_type='����' 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='ҵ��С��' ;
                --where not exists (select 1 from ts_county_info_15 a1 where a1.createtime=t.createtime and a1.countyid=t.countyid and a1.at=t.at and a1.ast=t.ast);
                commit;
                
              --  insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(  insert into ts_county_info_15)','ִ�н���');
              --  commit;
                
                /*  �����±��۵�ʱ�䡢�����������ȼ�¼��ts_dataimport_record�У�����ǰ̨չʾʹ��
                ts_county_info_15
                */
               for cur_cell in (select relatetable from ts_enum_table_relation where basetable ='ts_county_info_15_tp'
                 )loop
                 
                        v_table:=cur_cell.relatetable;
                        v_sql:='insert into ts_dataimport_record(createtime,table_name,cnt) select * from (select :x1 as createtime,:x2 as table_name,count(*) as cnt from '||v_table||' t where createtime=:x1 ) t1 where not exists (select 1 from ts_dataimport_record t2 where t1.createtime=t2.createtime and t2.table_name=:x2)';
                        execute immediate v_sql using v_createtime,v_table,v_createtime,v_table;
                        commit;
                       -- DBms_output.put_line(v_table||'----'||v_createtime||'-----'||v_sql);
               end loop;
               
              -- insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_city_info_15_tp )','ִ�н���');
              -- commit;  
              
              
        -- add by dufs 2018-6-28 ŵ��ÿ�������ṩ��vipС�����ݣ�ȷ�ϲ�����vipС�����ٵ����
        when TYPE='CELL_VIP'
        then
                merge into ts_cell_vip t
                using (select eci,cgi,eci_name,traffic_al from (select eci,cgi,eci_name,traffic_al,row_number() over(partition by c.cgi order by traffic_al desc) rn from ts_cell_vip_tp c) where rn=1) tp on (t.cgi=tp.cgi)
                when matched then update set t.eci=tp.eci,t.eci_name=tp.eci_name,t.traffic_al=tp.traffic_al
                when not matched then insert (eci,cgi,eci_name,traffic_al) values(tp.eci,tp.cgi,tp.eci_name,tp.traffic_al);
                commit;
                
                delete from ts_cell_vip where rowid in (select rowid from (select rowid,row_number() over(partition by c.eci order by traffic_al desc) rn from ts_cell_vip c) where rn>1);
                commit;
        end case;                

    exception when others then
    v_logInfo := sqlerrm;
    v_logInfo := substr(v_logInfo, 0, 1999);
    execute immediate 'insert into prd_exec_log(prd,what,logtype,loginfo) values (:x1,:x2,:x3,:x4)' 
              using 'TS_COLLECT_PRD','TS_COLLECT_PRD('||TYPE||')','ERROR',v_loginfo;
    commit;

end; 

/
