--/
CREATE PROCEDURE TS_COLLECT_PRD( TYPE in varchar2)
AS

-- add by dufs 2017-05-05
-- 综合区域保障场景 数据处理
 
-- 调用方式 211.4服务器上crontab 任务
-- #=========================== 综合区域保障场景数据接入 =========================================================
-- 3,18,33,48 * * * * . $HOME/.profile; cd /backup/ts_collect; ./ts_city_info_15_Run.sh >/dev/null 2>&1
-- #=========================== 综合区域保障场景数据接入 =========================================================

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
                
             --   insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(  insert  into ts_city_info_15)','开始执行');

             --15分钟地市业务聚合表
                insert /* + append */ into ts_city_info_15 (createtime,cityid,city,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)
                select createtime,cityid,m.enum_value,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500
                from ts_city_info_15_tp t
                left join ts_enum m on t.cityid=m.enum_key and m.enum_type='地市' 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类' ;
                --where not exists (select 1 from ts_city_info_15 a1 where a1.createtime=t.createtime and a1.cityid=t.cityid and a1.at=t.at and a1.ast=t.ast);
                commit;
                
              --  insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(  insert  into ts_city_info_15)','执行结束');
              --  commit;
                
                TS_CITY_COUNT_PRD('AST');
              --  insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_CITY_COUNT_PRD - AST )','执行结束');
              --  commit;
                
                TS_CITY_COUNT_PRD('AT');
             --   insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_CITY_COUNT_PRD - AT )','执行结束');
             --   commit;

                /*  将以下表汇聚的时间、表名、数量等记录到ts_dataimport_record中，用于前台展示使用
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
               
              -- insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_city_info_15_tp )','执行结束');
              -- commit;

               TS_ALARM_PRD_NEW('PROV_ACC_SUCC');
               
               -- add by dufs 2018-5-29
               -- 省、地市业务大类、业务小类指标的业务访问成功率、下载流量、速率异常告警（较上周去掉最高、最低后平均值下降30%）
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
                
                -- 有多个数据文件，分文件处理数据，导入tp表中，关联小区和场景后写入ts_cell_info_15
                insert into ts_cell_info_15_tp (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)                
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012')
                ;
                commit;
                
                /*
                union
                -- 2017-10-26 增加居民区小区数据，为汇聚区县级居民区指标准备
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类'
                where exists (select 1 from rm_eutrancell eu where eu.objectid=r.objectid and eu.scencetype in ('低层居民区','别墅群','高层居民区','城中村'))
                ;
                commit;
                
                */
                
                insert /* + append */ into ts_cell_info_15 (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)                
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012')
                ;
                commit;
                
                /*
                -- 有多个数据文件，分文件处理数据，导入tp表中，关联小区和场景后写入ts_cell_info_15
                insert into ts_cell_info_15 (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)                
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                join rm_eutrancell_eci_n r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012')
                 or r.scencetype in ('低层居民区','别墅群','高层居民区','城中村')
                union
                -- 2017-10-26 增加居民区小区数据，为汇聚区县级居民区指标准备
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500                
                from ts_cell_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类'
                where exists (select 1 from rm_eutrancell eu where eu.objectid=r.objectid and eu.scencetype in ('低层居民区','别墅群','高层居民区','城中村'))
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
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_info_15  )_3_1','开始执行');

             --15分钟小区业务大小类
             /*
                insert into ts_cell_info_15 (createtime,eci,cell_id,cell_name,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2)
                select createtime,t.eci,r.objectid,r.objectname,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2
                from ts_cell_info_15_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类' ;
              --  where not exists (select 1 from ts_cell_info_15 a1 where a1.createtime=t.createtime and a1.eci=t.eci and a1.at=t.at and a1.ast=t.ast) ;
              */

                -- dufs 2018-1-24 这个有时候要执行将近5分钟， 改成在CELL_PART里分批写入的方式，30多秒
                --insert into ts_cell_info_15 select * from ts_cell_info_15_tp;
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_info_15  )_3_2','执行结束');
                commit;

                TS_AREA_COUNT_PRD('AST');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_AREA_COUNT_PRD - AST )_3_3','执行结束');
                commit;

                TS_SEN_COUNT_PRD('AST');      
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_SEN_COUNT_PRD - AST )_3_4','执行结束');
                commit;
                
                --指定业务小类，按区县汇聚数据
                TS_COUNTY_COUNT_PRD('AST');

                /*将以下表汇聚的时间、表名、数量等记录到ts_dataimport_record中，用于前台展示使用
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
               
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_cell_info_15_tp )_3_5','执行结束');
               commit;
               
       
        when TYPE='CELL_DZG'
        then
                -- 业务大类”其中“，业务小类”大掌柜“，每小时1份数据，需要自动复制生成每15分钟1份数据
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
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012');
                commit;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_info_15_dzg  )','执行结束');
                commit;

                TS_AREA_COUNT_PRD('DZG');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_AREA_COUNT_PRD - DZG )','执行结束');
                commit;

                TS_SEN_COUNT_PRD('DZG');      
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_SEN_COUNT_PRD - DZG )','执行结束');
                commit;  
                             
        when TYPE='CELL_APPTYPE_PART'
        then
                -- 有多个数据文件，分文件处理数据，导入tp表中，关联小区和场景后写入ts_cell_apptype_info_15
                insert into ts_cell_apptype_info_15_tp (createtime,eci,cell_id,cell_name,at,at_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)
                select createtime,t.eci,r.objectid,r.objectname,at,m1.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500
                from ts_cell_apptype_info_15_tp_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m1 on t.at=m1.enum_key and m1.enum_type='业务大类'
                where exists (select 1 from rm_senareane_space_mv a where r.objectid=a.object_id and a.sen_type_id='ST0012');
                commit;
              
        when TYPE='CELL_APPTYPE'
        then
                select max(createtime) into v_createtime from ts_cell_apptype_info_15_tp;
                
                if v_createtime is null
                then
                      return;
                end if;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_apptype_info_15  )_1_1','开始执行');
                commit;

             --15分钟小区业务大类
             --2017-5-28 只保留与场景相关的小区数据
             /*
                insert into ts_cell_apptype_info_15 (createtime,eci,cell_id,cell_name,at,at_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500)
                select createtime,t.eci,r.objectid,r.objectname,at,m1.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2,
                       common_ul,common_dl,common_al,http_traffic_dl,http_traffic_dl_010,http_traffic_dl_1050,http_traffic_dl_50100,http_traffic_dl_100500,http_traffic_dl_500,http_dura_dl,http_dura_dl_010,http_dura_dl_1050,http_dura_dl_50100,http_dura_dl_100500,http_dura_dl_500,http_xdrc,http_xdrc_010,http_xdrc_1050,http_xdrc_50100,http_xdrc_100500,http_xdrc_500,http_dl_rate,http_dl_rate_010,http_dl_rate_1050,http_dl_rate_50100,http_dl_rate_100500,http_dl_rate_500,delta_http_traffic_dl,delta_http_traffic_dl_010,delta_http_traffic_dl_1050,delta_http_traffic_dl_50100,delta_http_traffic_dl_100500,delta_http_traffic_dl_500,delta_http_dl_rate,delta_http_dl_rate_010,delta_http_dl_rate_1050,delta_http_dl_rate_50100,delta_http_dl_rate_100500,delta_http_dl_rate_500,http_traffic_ul,http_traffic_ul_010,http_traffic_ul_1050,http_traffic_ul_50100,http_traffic_ul_100500,http_traffic_ul_500,http_dura_ul,http_dura_ul_010,http_dura_ul_1050,http_dura_ul_50100,http_dura_ul_100500,http_dura_ul_500,http_ul_rate,http_ul_rate_010,http_ul_rate_1050,http_ul_rate_50100,http_ul_rate_100500,http_ul_rate_500,delta_http_traffic_ul,delta_http_traffic_ul_010,delta_http_traffic_ul_1050,delta_http_traffic_ul_50100,delta_http_traffic_ul_100500,delta_http_traffic_ul_500,delta_http_ul_rate,delta_http_ul_rate_010,delta_http_ul_rate_1050,delta_http_ul_rate_50100,delta_http_ul_rate_100500,delta_http_ul_rate_500
                from ts_cell_apptype_info_15_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci 
                left join ts_enum m1 on t.at=m1.enum_key and m1.enum_type='业务大类'
                where exists (select 1 from tscene_sre sre join tscene_sba sba on sre.be_id=sba.area_id and sba.sen_type_id='ST0012' join tscene_areainfo area on sba.area_id=area.area_id where sre.object_id=r.objectid);
                commit;
                */
                insert /* + append */ into ts_cell_apptype_info_15 select * from ts_cell_apptype_info_15_tp;
                commit;
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert  into ts_cell_apptype_info_15  )_1_2','执行结束');
                commit;

                TS_AREA_COUNT_PRD('AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_AREA_COUNT_PRD - AT )_1_3','执行结束');
                commit;
                
                TS_SEN_COUNT_PRD('AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_SEN_COUNT_PRD - AT )_1_4','执行结束');
                commit;

                --小区业务大类指标预警生成
                TS_RULE_WARN_PRD('CELL_AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - CELL_AT )_1_5','执行结束');
                commit;

                --区域业务大类指标预警生成
                TS_RULE_WARN_PRD('AREA_AT');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - AREA_AT )_1_6','执行结束');
               commit;   

                 /*将以下表汇聚的时间、表名、数量等记录到ts_dataimport_record中，用于前台展示使用
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
               
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_cell_apptype_info_15 )_1_7','执行结束');
               commit;   
               
               -- 基于小区预警生成衍生告警
               -- TS_ALARM_PRD();
               --TS_ALARM_PRD_NEW('AREA'); --被5包规则替换
               --insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(AREA) )_1_8','执行结束');
               --TS_ALARM_PRD_NEW('CITY'); --被5包规则替换
               --insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(CITY) )_1_9','执行结束');               
               
               --TS_ALARM_PRD_NEW('CELL'); --被5包规则替换
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(CELL) )_1_10','执行结束');  
               TS_ALARM_PRD_NEW('CELL_5PKG');
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(TS_ALARM_PRD(CELL_5PKG) )_1_11','执行结束'); 
               
               --add by dufs-2018-2-8 这个要在TS_ALARM_PRD_NEW('CELL_5PKG')的基础上执行
               -- 小区5包区域累计达到1/3
               TS_ALARM_PRD_NEW('CELL_AREA_5PKG');
               -- 小区5包地市累计达200次或1/10小区
               TS_ALARM_PRD_NEW('CELL_CITY_5PKG'); 
               -- 小区5包当天累计达到20次
               TS_ALARM_PRD_NEW('CELL_5PKG_TOTAL'); 
               
               -- 处理时判断了ts_dataimport_record表里有最近的ts_cell_common_info_15更新记录，所有要放到下面来
               TS_ALARM_PRD_NEW('CELL_OVERFLOW');
               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_ALARM_PRD_NEW - CELL_OVERFLOW )_2_11','执行结束');
               commit;   
                               
               -- 记录更新时间
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
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_common_info_15_tp )_2_1','开始执行');                
                commit;
                
             --15分钟小区通用指标
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
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_common_info_15_tp )_2_2','执行结束');                
                commit;                
                
                -- 先汇地市级别的，然后把场景外的小区从tp表中删掉，提高后续基于该表的操作效率。
                TS_CITY_COUNT_PRD('COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_CITY_COUNT_PRD - COMMON )_2_3','执行结束');
                commit; 
                
                /*
                delete from ts_cell_common_info_15_tp t 
                where not exists (select 1 from tscene_sre sre join tscene_sba sba on sre.be_id=sba.area_id and sba.sen_type_id='ST0012' join tscene_areainfo area on sba.area_id=area.area_id join rm_eutrancell_eci r1 on sre.object_id=r1.objectid 
                                  where r1.eci=t.eci);
                commit;
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( delete from ts_cell_common_info_15_tp t not exists)_2_4','执行结束');                
                */
                TS_AREA_COUNT_PRD('COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_AREA_COUNT_PRD - COMMON )_2_5','执行结束');                
                commit;   
                
                TS_SEN_COUNT_PRD('COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_SEN_COUNT_PRD - COMMON )_2_6','执行结束');                
                commit;   



                --小区通用指标预警生成
                TS_RULE_WARN_PRD('CELL_COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - CELL_COMMON )_2_7','执行结束');
                commit;      
                
                -- add by dufs 2017-11-9 只针对双十一保障期间，生成居民区小区的通用预警
                if (sysdate >=to_date('2017-11-09','yyyy-mm-dd') and sysdate<to_date('2017-11-12','yyyy-mm-dd'))
                then 
                        TS_RULE_WARN_PRD('CELL_COMMON_JMQ');
                        insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - CELL_COMMON_JMQ )_2_8','执行结束');
                        commit;
                end if;
                
                 --区域通用指标预警生成
                TS_RULE_WARN_PRD('AREA_COMMON');
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( TS_RULE_WARN_PRD - AREA_COMMON )_2_9','执行结束');
                commit;   
                                               
                 /*将以下表汇聚的时间、表名、数量等记录到ts_dataimport_record中，用于前台展示使用
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

               insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_cell_common_info_15_tp )_2_10','执行结束');
               commit;   
 
                -- 下面的操作，要判断ts_dataimport_record表里最新记录的ts_cell_common_info_15表的信息，所以放在需要放到最后
                -- 通用业务HTTP访问成功率低于一级门限 VVIP小区
                TS_ALARM_PRD_NEW('CELL_APP_SUCC');
                -- 通用业务HTTP平均下载速率低于一级门限 VVIP小区
                TS_ALARM_PRD_NEW('CELL_HTTP_DL_RATE');
                -- 通用区县大面积低流量告警
                TS_ALARM_PRD_NEW('CELL_COUNTY_FLOW_LOW');
                -- 通用地市大面积低流量告警
                TS_ALARM_PRD_NEW('CELL_CITY_FLOW_LOW');
                
                              

        -- 按地市、场景汇聚每小时汇聚的用户数
        when TYPE='cityScene'
        then
                select max(createtime) into v_createtime from ts_cityscene_usercount_tp;
                if v_createtime is null
                then
                      return;
                end if;
                
                insert /* + append */ into ts_cityscene_usercount (createtime,cityid,city,sen_id,sen_name,user_count,granularity,object_type)
                select t.createtime,t.cityid,m.enum_value,t.sen_id,s.sen_name,user_count,granularity,
                case when t.cityid=0 and t.sen_id='8888' then '省'
                     when t.cityid=0 and t.sen_id is not null and t.sen_id!='8888' then '省+场景'
                     when t.cityid!=0 and t.sen_id='8888' then '地市'
                     when t.cityid!=0 and t.sen_id is not null and t.sen_id!='8888' then '地市+场景'
                end object_type
                from ts_cityscene_usercount_tp t
                left join ts_enum m on t.cityid=m.enum_key and m.enum_type='地市'
                left join tscene_seninfo s on s.sen_id=t.sen_id
                ;
                commit;

        -- 按地市、场景、区域汇聚每小时汇聚的用户数
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
                left join ts_enum m on t.cityid=m.enum_key and m.enum_type='地市'
                left join tscene_seninfo s on s.sen_id=t.sen_id
                left join tscene_areainfo a on a.area_id=t.area_id
                ;
                commit;                

        -- 每日小区告警接入
        when TYPE='CELL_ALARM'
        then
                select max(createtime) into v_createtime from ts_cell_alarm_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_alarm_tp )','开始执行');                
                commit;

                insert into ts_cell_alarm (createtime,city,eci,cell_name,cell_id,at,ast,at_name,ast_name,alarm_count,is_alarm,alarm_title,alarm_rule,alarm_level,alarm_info,statnum)
                select t.createtime,r.regionname,t.eci,t.cell_name,r.objectid,t.at,t.ast,t.at_name,t.ast_name,t.alarm_count,t.is_alarm,t.alarm_title,t.alarm_rule,t.alarm_level,t.alarm_info,statnum
                from ts_cell_alarm_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci ;
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_cell_alarm_tp )','执行结束');                
                commit;
                
        -- 每日小区清除告警接入
        when TYPE='CELL_CLEAR'
        then
                select max(createtime) into v_createtime from ts_cell_clear_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_cell_clear_tp )','开始执行');                
                commit;
                
                --根据ECI和at_code和alarm_title，匹配7天以内的告警信息，将匹配上的告警的是否清除（is_clear）信息更新为1
                update ts_cell_alarm t set is_clear=0,clear_time=(select max(createtime) from ts_cell_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title)
                where t.createtime>=trunc(v_createtime)-7 and t.createtime<trunc(v_createtime)
                and exists (select 1 from ts_cell_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title);
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_cell_clear_tp )','执行结束');                
                commit;  
                

        -- 每日区域告警接入
        when TYPE='AREA_ALARM'
        then
                select max(createtime) into v_createtime from ts_area_alarm_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_area_alarm_tp )','开始执行');                
                commit;

                insert into ts_area_alarm (createtime,city,eci,cell_name,cell_id,area_id,area_name,at,ast,at_name,ast_name,alarm_count,is_alarm,alarm_title,alarm_rule,alarm_level,alarm_info)
                select t.createtime,r.regionname,t.eci,t.cell_name,r.objectid,t.area_id,t.area_name,t.at,t.ast,t.at_name,t.ast_name,t.alarm_count,t.is_alarm,t.alarm_title,t.alarm_rule,t.alarm_level,t.alarm_info
                from ts_area_alarm_tp t
                left join rm_eutrancell_eci r on t.eci=r.eci ;
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( insert into ts_area_alarm_tp )','执行结束');                
                commit;
                
        -- 每日区域清除告警接入
        when TYPE='AREA_CLEAR'
        then
                select max(createtime) into v_createtime from ts_area_clear_tp;
                if v_createtime is null
                then
                      return;
                end if;                
                
                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_area_clear_tp )','开始执行');                
                commit;
                
                --根据ECI和at_code和alarm_title，匹配7天以内的告警信息，将匹配上的告警的是否清除（is_clear）信息更新为1
                update ts_area_alarm t set is_clear=0,clear_time=(select max(createtime) from ts_area_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title)
                where t.createtime>=trunc(v_createtime)-7 and t.createtime<trunc(v_createtime)
                and exists (select 1 from ts_area_clear_tp c where t.eci=c.eci and t.at=c.at and t.alarm_title=c.alarm_title);
                commit;

                insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD( update by ts_area_clear_tp )','执行结束');                
                commit;  

        when TYPE='COUNTY'
        then
                select max(createtime) into v_createtime from ts_county_info_15_tp ;
                if v_createtime is null
                then
                      return;
                end if;
                
             --15分钟区县业务聚合表
                insert /* + append */ into ts_county_info_15 (createtime,countyid,county,at,ast,at_name,ast_name,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2)
                select createtime,countyid,m.enum_value,at,ast,m2.relate_enum_value,m2.enum_value,app_accept,app_attemp,app_succ,total_ul,total_dl,total_al,total_duration,total_rate,ty_dl,ty_duration,ty_rate,user_count,http_pg_accept,http_pg_attemp,http_pg_succ,tra_pg,tra_pg_duration,tra_pg_rate,http_im_accept,http_im_attemp,http_im_succ,tra_im_ul,tra_im_dl,tra_im_duration,tra_im_rate_up,tra_im_rate_down,http_vd_accept,http_vd_attemp,http_vd_succ,tra_vd,tra_vd_duration,tra_vd_rate,http_ad_accept,http_ad_attemp,http_ad_succ,tra_ad,tra_ad_duration,tra_ad_rate,delta_tra1,delta_rate1,delta_tra2,delta_rate2
                from ts_county_info_15_tp t
                left join ts_enum m on t.countyid=m.enum_key and m.enum_type='区县' 
                left join ts_enum m2 on t.ast=m2.enum_key and t.at=m2.relate_enum_key and m2.enum_type='业务小类' ;
                --where not exists (select 1 from ts_county_info_15 a1 where a1.createtime=t.createtime and a1.countyid=t.countyid and a1.at=t.at and a1.ast=t.ast);
                commit;
                
              --  insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(  insert into ts_county_info_15)','执行结束');
              --  commit;
                
                /*  将以下表汇聚的时间、表名、数量等记录到ts_dataimport_record中，用于前台展示使用
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
               
              -- insert into procedures_exec_record_pf (prd,what)values('TS_COLLECT_PRD(ts_dataimport_record - ts_city_info_15_tp )','执行结束');
              -- commit;  
              
              
        -- add by dufs 2018-6-28 诺西每周增量提供的vip小区数据，确认不考虑vip小区减少的情况
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
