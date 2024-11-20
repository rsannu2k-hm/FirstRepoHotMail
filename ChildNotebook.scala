// Databricks notebook source
dbutils.widgets.text("load_group", "", "load_group")
val a = dbutils.widgets.get("load_group")
println(a)

// COMMAND ----------

// MAGIC %sql
// MAGIC select src_sys_cd, count(1) from dw_xle_az.t_f_xle_policy_transaction_summary group by src_sys_cd;

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select src_sys_cd, count(1)  from dw_xle_az.t_ff_xle_snapshot_policy group by src_sys_cd;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) from dw_xle_az.t_ff_xle_snapshot_policy where plcy_id not in(select plcy_id from dw_xle_az.t_f_xle_policy_transaction_summary)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a.plcy_id),a.src_sys_cd  from dw_xle_az.t_ff_xle_snapshot_policy a left outer join dw_xle_az.t_f_xle_policy_transaction_summary b on a.plcy_id = b.plcy_id and a.src_sys_cd = b.src_sys_cd   group by a.src_sys_cd

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a.plcy_id),a.src_sys_cd  from dw_xle_az.t_ff_xle_snapshot_policy a left outer join dw_xle_az.t_f_xle_policy_transaction_summary b on a.plcy_id = b.plcy_id and a.src_sys_cd = b.src_sys_cd where b.plcy_id is null  group by a.src_sys_cd

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a_plcy_id),src_sys_cd from (select a.plcy_id as a_plcy_id, b.plcy_id as b_plcy_id,  a.src_sys_cd from dw_xle_az.t_ff_xle_snapshot_policy a left outer join dw_xle_az.t_f_xle_policy_transaction_summary b on a.plcy_id = b.plcy_id and a.src_sys_cd = b.src_sys_cd) where b_plcy_id is null  group by src_sys_cd

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a_plcy_id),plcy_src_sys_cd from (select a.plcy_id as a_plcy_id, b.plcy_id as b_plcy_id,  a.plcy_src_sys_cd from dw_xle_sz.t_d_xle_policy a left outer join dw_xle_az.t_f_xle_policy_transaction_summary b on a.plcy_id = b.plcy_id and a.plcy_src_sys_cd = b.src_sys_cd) where b_plcy_id is null  group by plcy_src_sys_cd

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a_plcy_id),plcy_src_sys_cd from (select a.plcy_id as a_plcy_id, b.plcy_id as b_plcy_id,  a.plcy_src_sys_cd from dw_xle_sz.t_d_xle_policy a 
// MAGIC inner join dw_xle_sz.t_d_xle_source_system c on a.plcy_src_sys_cd = c.src_sys_cd
// MAGIC left outer join dw_xle_sz.t_f_xle_policy_transaction_detail_as_is b on a.plcy_id = b.plcy_id and c.src_sys_id = b.src_sys_id
// MAGIC where a.dw_row_active_ind = 'Y') where b_plcy_id is null  group by plcy_src_sys_cd

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) from dw_xle_sz.t_d_xle_policy where dw_row_active_ind = 'Y';

// COMMAND ----------

// MAGIC %sql
// MAGIC select plcy_src_sys_cd, count(1)  from dw_xle_sz.t_d_xle_policy a left outer join dw_xle_az.t_ff_xle_snapshot_policy b
// MAGIC on a.plcy_id = b.plcy_id 
// MAGIC where a.dw_row_active_ind = 'Y' and b.plcy_id is null group by plcy_src_sys_cd;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dw_xle_sz.t_d_xle_source_system where src_sys_cd in( 'ACT', 'PRC','SUB');
