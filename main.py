import os
import appbuilder
from appbuilder.core.message import Message
from appbuilder.core.components.gbi.basic import SessionRecord
from db_connect import mysql_connect_pool
#  设置环境变量
os.environ["APPBUILDER_TOKEN"] = "your tokens"

dag_schema_info = """
CREATE TABLE `dag` (
    `dag_id` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8_bin NOT NULL COMMENT 'DAG的唯一标识',
    `is_paused` tinyint(1) DEFAULT NULL COMMENT '是否暂停',
    `is_subdag` tinyint(1) DEFAULT NULL COMMENT '是否为子DAG',
    `is_active` tinyint(1) DEFAULT NULL COMMENT '是否活跃',
    `last_parsed_time` timestamp NULL DEFAULT NULL COMMENT '最后解析时间',
    `last_pickled` timestamp(6) NULL DEFAULT NULL COMMENT '最后序列化时间',
    `last_expired` timestamp(6) NULL DEFAULT NULL COMMENT '最后过期时间',
    `scheduler_lock` tinyint(1) DEFAULT NULL COMMENT '调度器锁',
    `pickle_id` int DEFAULT NULL COMMENT '序列化ID',
    `fileloc` varchar(2000) DEFAULT NULL COMMENT '文件位置',
    `owners` varchar(2000) DEFAULT NULL COMMENT '所有者',
    `description` text COMMENT '描述',
    `default_view` varchar(25) DEFAULT NULL COMMENT '默认视图',
    `schedule_interval` text COMMENT '调度间隔',
    `root_dag_id` varchar(250) DEFAULT NULL COMMENT '根DAG的ID',
    `next_dagrun` timestamp(6) NULL DEFAULT NULL COMMENT '下一次DAG运行时间',
    `next_dagrun_create_after` timestamp(6) NULL DEFAULT NULL COMMENT '下一次DAG运行创建后时间',
    `max_active_tasks` int NOT NULL COMMENT '最大活动任务数',
    `has_task_concurrency_limits` tinyint(1) NOT NULL COMMENT '是否有任务并发限制',
    `max_active_runs` int DEFAULT NULL COMMENT '最大活动运行数',
    `next_dagrun_data_interval_start` timestamp(6) NULL DEFAULT NULL COMMENT '下一次DAG运行数据间隔开始时间',
    `next_dagrun_data_interval_end` timestamp(6) NULL DEFAULT NULL COMMENT '下一次DAG运行数据间隔结束时间',
    `has_import_errors` tinyint(1) DEFAULT '0' COMMENT '是否有导入错误',
    `timetable_description` varchar(1000) DEFAULT NULL COMMENT '时间表描述',
    PRIMARY KEY (`dag_id`),
    KEY `idx_root_dag_id` (`root_dag_id`),
    KEY `idx_next_dagrun_create_after` (`next_dagrun_create_after`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

"""
dag_run_schema_info = """
CREATE TABLE `dag_run` (
    `id` int NOT NULL AUTO_INCREMENT COMMENT '唯一标识ID',
    `dag_id` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8_bin NOT NULL COMMENT 'DAG的唯一标识',
    `execution_date` timestamp(6) NOT NULL COMMENT '执行日期',
    `state` varchar(50) DEFAULT NULL COMMENT '运行状态',
    `run_id` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8_bin NOT NULL COMMENT '运行ID',
    `external_trigger` tinyint(1) DEFAULT NULL COMMENT '外部触发器标志',
    `conf` blob COMMENT '配置信息',
    `end_date` timestamp(6) NULL DEFAULT NULL COMMENT '结束日期',
    `start_date` timestamp(6) NULL DEFAULT NULL COMMENT '开始日期',
    `run_type` varchar(50) NOT NULL COMMENT '运行类型',
    `last_scheduling_decision` timestamp(6) NULL DEFAULT NULL COMMENT '最后调度决策时间',
    `dag_hash` varchar(32) DEFAULT NULL COMMENT 'DAG哈希值',
    `creating_job_id` int DEFAULT NULL COMMENT '创建作业的ID',
    `queued_at` timestamp(6) NULL DEFAULT NULL COMMENT '排队时间',
    `data_interval_start` timestamp(6) NULL DEFAULT NULL COMMENT '数据间隔开始时间',
    `data_interval_end` timestamp(6) NULL DEFAULT NULL COMMENT '数据间隔结束时间',
    `log_template_id` int DEFAULT NULL COMMENT '日志模板ID',
    PRIMARY KEY (`id`),
    UNIQUE KEY `dag_run_dag_id_execution_date_key` (`dag_id`,`execution_date`),
    UNIQUE KEY `dag_run_dag_id_run_id_key` (`dag_id`,`run_id`),
    KEY `dag_id_state` (`dag_id`,`state`),
    KEY `idx_last_scheduling_decision` (`last_scheduling_decision`),
    KEY `idx_dag_run_dag_id` (`dag_id`),
    KEY `idx_dag_run_running_dags` (`state`,`dag_id`),
    KEY `idx_dag_run_queued_dags` (`state`,`dag_id`),
    KEY `task_instance_log_template_id_fkey` (`log_template_id`),
    CONSTRAINT `task_instance_log_template_id_fkey` FOREIGN KEY (`log_template_id`) REFERENCES `log_template` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=393566 DEFAULT CHARSET=utf8mb3;

"""

dag_tag_schema_info = """
CREATE TABLE `dag_tag` (
    `name` varchar(100) NOT NULL COMMENT '标签名称',
    `dag_id` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8_bin NOT NULL COMMENT '关联的DAG的唯一标识',
    PRIMARY KEY (`name`,`dag_id`) COMMENT '主键，由标签名称和DAG标识组成',
    KEY `dag_id` (`dag_id`),
    CONSTRAINT `dag_tag_ibfk_1` FOREIGN KEY (`dag_id`) REFERENCES `dag` (`dag_id`) COMMENT '外键约束，引用dag表的dag_id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

"""
# schema和表名的映射
TABLE_SCHEMA_MAPPING = {    
    "dag": dag_schema_info,
    "dag_run": dag_run_schema_info,
    "dag_tag": dag_tag_schema_info
}

# 设置表的描述用于选表
table_descriptions = {
    "dag": "用于记录airflow dag信息的表",
    "dag_run": "用于记录airflow dag的运行情况的表",
    "dag_tag": "用于记录airflow dag的标签信息的表"
}

MODEL_NAME = "ERNIE-Bot 4.0"

# 选表
def select_table(query):
    select_table = appbuilder.SelectTable(model_name=MODEL_NAME, table_descriptions=table_descriptions)    
    msg = Message({"query": query})
    select_table_result_message = select_table(msg)
    print(f"选的表是: {select_table_result_message.content}")
    return select_table_result_message

def ask_table(query):
    select_table_result_message = select_table(query)
    # 选择的表，前后可能携带空格，做特殊处理
    table_schemas = [TABLE_SCHEMA_MAPPING[table_name.strip()] for table_name in select_table_result_message.content]
    gbi_nl2sql = appbuilder.NL2Sql(model_name=MODEL_NAME, table_schemas=table_schemas)
    
    # 添加知识
    gbi_nl2sql.knowledge["任务名"] = "对应gag表的dag_id"

    nl2sql_result_message = gbi_nl2sql(Message({"query": query}))
    print(f"执行的sql是: {nl2sql_result_message.content.sql}")
    # print("-----------------")
    # print(f"llm result: {nl2sql_result_message.content.llm_result}")
    return nl2sql_result_message.content.sql

if __name__ == "__main__":
    # 测试单表查询
    QUERY_1 = "查询任务ai_issue_level的最近一次运行是否成功"
    sql_query = ask_table(query=QUERY_1)
    result = mysql_connect_pool.execute_query(sql_query)
    print(f"数据库返回结果是: {result}")
    
    # 测试多表联合检索
    QUERY_2 = "查询标签包含产品周报的dag最近一天的运行情况"
    sql_query = ask_table(query=QUERY_2)
    result = mysql_connect_pool.execute_query(sql_query)
    print(f"数据库返回结果是: {result}")