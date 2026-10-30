/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_ai_service.h"
#include "observer/ai_service/ob_ai_service_executor.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "lib/time/ob_time_utility.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
namespace oceanbase
{
namespace omt
{

ObAiService::ObAiService()
  : is_inited_(false)
{
}

ObAiService::~ObAiService()
{
  destroy();
}

int ObAiService::server_module_init(ObAiService* &ai_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ai_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ai_service->init())) {
  }
  return ret;
}

int ObAiService::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    if (OB_SUCC(ret = set_thread_count(4))) { is_inited_ = true; }
  }
  return ret;
}

void ObAiService::destroy()
{
  stop();
  wait();
  is_inited_ = false;
}


namespace
{
struct AiJob
{
  uint64_t table_id = 0, column_id = 0, state_id = 0, request_id = 0;
  int64_t definition_version = 0, retries = 0, attempts = 0, state = 0, eligible = 1;
  ObString request, predicate, db, table, column, expr, result, state_name, request_name;
  bool result_null = true;
};

int append_ai_hex(ObSqlString &sql, const ObString &value)
{
  int ret = sql.append("X'");
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    ret = sql.append_fmt("%02x", static_cast<unsigned char>(value.ptr()[i]));
  }
  if (OB_SUCC(ret)) { ret = sql.append("'"); }
  return ret;
}

int append_ai_identifier(ObSqlString &sql, const ObString &name)
{
  int ret = sql.append("`");
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    ret = sql.append(name.ptr() + i, 1);
    if (OB_SUCC(ret) && name.ptr()[i] == '`') { ret = sql.append("`"); }
  }
  if (OB_SUCC(ret)) { ret = sql.append("`"); }
  return ret;
}

int append_ai_result_text(ObSqlString &sql,const ObString &value)
{
  // Hex literals otherwise have numeric/binary semantics in column casts.
  // Force decoded model content to text before JSON, number or vector casting.
  int ret=sql.append("CONVERT(");
  if(OB_SUCC(ret)){ret=append_ai_hex(sql,value);}
  if(OB_SUCC(ret)){ret=sql.append(" USING utf8mb4)");}
  return ret;
}

int append_ai_table(ObSqlString &sql, const AiJob &job)
{
  int ret = append_ai_identifier(sql, job.db);
  if (OB_SUCC(ret)) { ret = sql.append("."); }
  if (OB_SUCC(ret)) { ret = append_ai_identifier(sql, job.table); }
  return ret;
}

int append_ai_task_key(ObSqlString &sql, const AiJob &job, const char *owner = NULL)
{
  int ret = sql.append_fmt("table_id=%lu AND column_id=%lu AND request_id=", job.table_id, job.column_id);
  if (OB_SUCC(ret)) { ret = append_ai_hex(sql, job.request); }
  if (OB_SUCC(ret) && owner != NULL) { ret = sql.append_fmt(" AND lease_owner='%s'", owner); }
  return ret;
}

int append_ai_row_guard(ObSqlString &sql, const AiJob &job)
{
  int ret = sql.append_fmt("(%.*s) AND ", job.predicate.length(), job.predicate.ptr());
  if (OB_SUCC(ret)) { ret = append_ai_identifier(sql,job.request_name); }
  if (OB_SUCC(ret)) { ret = sql.append("="); }
  if (OB_SUCC(ret)) { ret = append_ai_hex(sql, job.request); }
  return ret;
}

int copy_ai_string(sqlclient::ObMySQLResult &row, const char *name, ObIAllocator &alloc, ObString &out)
{
  ObString value;
  int ret = row.get_varchar(name, value);
  if (OB_SUCC(ret)) { ret = ob_write_string(alloc, value, out); }
  return ret;
}

int load_ai_group(ObISQLClient &client, uint64_t table, const ObString &request,
                  ObIAllocator &alloc, ObIArray<AiJob> &jobs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_SUCC(ret = sql.append(
      "SELECT q.table_id,q.column_id,q.request_id,q.rowkey_predicate,q.definition_version,q.attempts,q.state,q.result,(q.eligible_ts<=now(6)) AS eligible_now,"
      "a.normalized_expr,a.max_retries,a.state_column_id,a.request_column_id,"
      "t.table_name,d.database_name,c.column_name,s.column_name AS state_name,r.column_name AS request_name FROM oceanbase.__all_ai_column_task q "
      "JOIN oceanbase.__all_ai_column a ON a.table_id=q.table_id AND a.column_id=q.column_id "
      "AND a.definition_version=q.definition_version "
      "JOIN oceanbase.__all_table t ON t.table_id=q.table_id "
      "JOIN oceanbase.__all_database d ON d.database_id=t.database_id "
      "JOIN oceanbase.__all_column s ON s.table_id=q.table_id AND s.column_id=a.state_column_id "
      "JOIN oceanbase.__all_column r ON r.table_id=q.table_id AND r.column_id=a.request_column_id "
      "JOIN oceanbase.__all_column c ON c.table_id=q.table_id AND c.column_id=q.column_id WHERE ")) &&
      OB_SUCC(ret = sql.append_fmt("q.table_id=%lu AND q.request_id=",table)) &&
      OB_SUCC(ret = append_ai_hex(sql,request)) && OB_SUCC(ret = sql.append(" ORDER BY q.column_id"))) {
    ObMySQLProxy::MySQLResult result;
    if (OB_SUCC(ret = client.read(result, sql.ptr()))) {
      sqlclient::ObMySQLResult *row = result.get_result();
      while (OB_SUCC(ret) && OB_SUCC(ret = row->next())) {
        AiJob job;
        int64_t value = 0;
        if (OB_SUCC(ret = row->get_int("table_id", value))) { job.table_id = value; }
        if (OB_SUCC(ret) && OB_SUCC(ret = row->get_int("column_id", value))) { job.column_id = value; }
        if (OB_SUCC(ret)) { ret = row->get_int("definition_version", job.definition_version); }
        if (OB_SUCC(ret)) { ret = row->get_int("max_retries", job.retries); }
        if (OB_SUCC(ret)) { ret = row->get_int("attempts", job.attempts); }
        if (OB_SUCC(ret)) { ret = row->get_int("state", job.state); }
        if (OB_SUCC(ret)) { ret = row->get_int("eligible_now", job.eligible); }
        if (OB_SUCC(ret) && OB_SUCC(ret = row->get_int("state_column_id", value))) { job.state_id = value; }
        if (OB_SUCC(ret) && OB_SUCC(ret = row->get_int("request_column_id", value))) { job.request_id = value; }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"request_id",alloc,job.request); }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"rowkey_predicate",alloc,job.predicate); }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"normalized_expr",alloc,job.expr); }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"database_name",alloc,job.db); }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"table_name",alloc,job.table); }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"column_name",alloc,job.column); }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"state_name",alloc,job.state_name); }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"request_name",alloc,job.request_name); }
        if (OB_SUCC(ret)) {
          ret = copy_ai_string(*row,"result",alloc,job.result);
          job.result_null = ret == OB_ERR_NULL_VALUE;
          if (job.result_null) { ret = OB_SUCCESS; }
        }
        if (OB_SUCC(ret)) { ret = jobs.push_back(job); }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  return ret;
}

int make_ai_update(const AiJob &job, ObSqlString &sql)
{
  int ret = sql.append("UPDATE ");
  if (OB_SUCC(ret)) { ret = append_ai_table(sql, job); }
  if (OB_SUCC(ret)) { ret = sql.append(" SET "); }
  if (OB_SUCC(ret)) { ret = append_ai_identifier(sql, job.column); }
  if (OB_SUCC(ret)) { ret = sql.append("="); }
  if (OB_SUCC(ret)) { ret = job.result_null ? sql.append("NULL") : append_ai_result_text(sql, job.result); }
  if (OB_SUCC(ret)) { ret = sql.append(" WHERE "); }
  if (OB_SUCC(ret)) { ret = append_ai_row_guard(sql, job); }
  return ret;
}

// Execute the native column conversion and index maintenance in a transaction
// that is always rolled back. This validates arbitrary supported result types
// without publishing any member of a still-running generation group.
int validate_ai_result(ObMySQLProxy &proxy, const AiJob &job)
{
  ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t affected = 0;
  int ret = make_ai_update(job,sql);
  if (OB_SUCC(ret)) { ret = trans.start(&proxy); }
  if (OB_SUCC(ret)) { ret = trans.write(sql.ptr(),affected); }
  if (trans.is_started()) { const int end_ret = trans.end(false); if (OB_SUCC(ret)) { ret = end_ret; } }
  return ret;
}

int run_ai_job(ObMySQLProxy &proxy, AiJob &job, ObIAllocator &alloc, const char *owner)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  // AI_COMPLETE exposes its content as JSON-encoded text; AI_EMBED exposes a
  // JSON array. Decode the transport string once before native result casting.
  if (OB_SUCC(ret = sql.append_fmt("SELECT JSON_UNQUOTE(CAST((%.*s) AS CHAR)) AS v FROM ", job.expr.length(),job.expr.ptr())) &&
      OB_SUCC(ret = append_ai_table(sql,job)) && OB_SUCC(ret = sql.append(" WHERE ")) &&
      OB_SUCC(ret = append_ai_row_guard(sql,job))) {
    ObMySQLProxy::MySQLResult result;
    if (OB_SUCC(ret = proxy.read(result,sql.ptr()))) {
      if (OB_SUCC(ret = result.get_result()->next())) {
        ret = copy_ai_string(*result.get_result(),"v",alloc,job.result);
        job.result_null = ret == OB_ERR_NULL_VALUE;
        if (job.result_null) { ret = OB_SUCCESS; }
      }
    }
  }
  if (OB_SUCC(ret)) { ret = validate_ai_result(proxy, job); }
  const int model_ret = ret;
  // A disappeared or superseded row is cancellation, not a new model failure.
  if (ret == OB_ITER_END) {
    sql.reset();
    if (OB_SUCC(ret = sql.append("DELETE FROM oceanbase.__all_ai_column_task WHERE ")) &&
        OB_SUCC(ret = append_ai_task_key(sql,job,owner))) {
      int64_t affected = 0;
      ret = proxy.write(sql.ptr(),affected);
    }
    return ret;
  }
  const bool retry = model_ret != OB_SUCCESS && job.attempts < job.retries;
  sql.reset();
  ret = sql.append_fmt("UPDATE oceanbase.__all_ai_column_task SET attempts=attempts+1,retry_count=retry_count+%d,error_code=%d,state=%d,result=",
                      (model_ret != OB_SUCCESS ? 1 : 0), model_ret, retry ? 3 : (model_ret == OB_SUCCESS ? 1 : 2));
  if (OB_SUCC(ret)) { ret = model_ret == OB_SUCCESS && !job.result_null ? append_ai_hex(sql,job.result) : sql.append("NULL"); }
  if (OB_SUCC(ret)) { ret = sql.append_fmt(",eligible_ts=date_add(now(6),INTERVAL %ld MICROSECOND) WHERE ",
                                          retry ? std::min<int64_t>(1000000,500000L << std::min<int64_t>(job.attempts,3)) : 0); }
  if (OB_SUCC(ret)) { ret = append_ai_task_key(sql,job,owner); }
  if (OB_SUCC(ret)) { int64_t affected = 0; ret = proxy.write(sql.ptr(),affected); }
  return ret;
}

int publish_ai_group(ObMySQLProxy &proxy, const ObIArray<AiJob> &jobs, const char *owner)
{
  int ret = OB_SUCCESS;
  if (jobs.empty()) { return ret; }
  bool pending = false;
  for (int64_t i = 0; i < jobs.count(); ++i) { pending |= (jobs.at(i).state == 0 || jobs.at(i).state == 3); }
  ObSqlString sql;
  int64_t affected = 0;
  if (pending) {
    if (OB_SUCC(ret = sql.append_fmt("UPDATE oceanbase.__all_ai_column_task SET lease_owner='',lease_until=NULL WHERE table_id=%lu AND request_id=",jobs.at(0).table_id)) &&
        OB_SUCC(ret = append_ai_hex(sql,jobs.at(0).request)) && OB_SUCC(ret = sql.append_fmt(" AND lease_owner='%s'",owner))) {
      ret = proxy.write(sql.ptr(),affected);
    }
    return ret;
  }
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(&proxy))) { return ret; }
  // Keep the claim fenced through publication. A lease that another worker has
  // acquired must never permit the previous owner to stage or publish results.
  if (OB_SUCC(ret = sql.append_fmt(
      "SELECT q.column_id FROM oceanbase.__all_ai_column_task q "
      "JOIN oceanbase.__all_ai_column a ON a.table_id=q.table_id AND a.column_id=q.column_id "
      "AND a.definition_version=q.definition_version WHERE q.table_id=%lu AND q.request_id=",jobs.at(0).table_id)) &&
      OB_SUCC(ret = append_ai_hex(sql,jobs.at(0).request)) && OB_SUCC(ret = sql.append_fmt(" AND q.lease_owner='%s' FOR UPDATE",owner))) {
    ObMySQLProxy::MySQLResult result;
    if (OB_SUCC(ret = trans.read(result,sql.ptr()))) {
      int64_t count = 0;
      while (OB_SUCC(ret = result.get_result()->next())) { ++count; }
      if (ret == OB_ITER_END) { ret = count == jobs.count() ? OB_SUCCESS : OB_EAGAIN; }
    }
  }
  sql.reset();
  if (OB_SUCC(ret)) { ret = sql.append("UPDATE "); }
  if (OB_SUCC(ret)) { ret = append_ai_table(sql,jobs.at(0)); }
  if (OB_SUCC(ret)) { ret = sql.append(" SET "); }
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    const AiJob &job = jobs.at(i);
    if (i > 0) { ret = sql.append(","); }
    if (OB_SUCC(ret)) { ret = append_ai_identifier(sql,job.column); }
    if (OB_SUCC(ret)) { ret = sql.append("="); }
    if (OB_SUCC(ret)) { ret = job.state == 1 && !job.result_null ? append_ai_result_text(sql,job.result) : sql.append("NULL"); }
    if (OB_SUCC(ret)) { ret = sql.append(","); }
    if (OB_SUCC(ret)) { ret = append_ai_identifier(sql,job.state_name); }
    if (OB_SUCC(ret)) { ret = sql.append_fmt("=%ld",job.state); }
  }
  if (OB_SUCC(ret)) { ret = sql.append(" WHERE "); }
  if (OB_SUCC(ret)) { ret = append_ai_row_guard(sql,jobs.at(0)); }
  for (int64_t i = 1; OB_SUCC(ret) && i < jobs.count(); ++i) {
    if (OB_SUCC(ret=sql.append(" AND ")) && OB_SUCC(ret=append_ai_identifier(sql,jobs.at(i).request_name)) &&
        OB_SUCC(ret=sql.append("="))) { ret=append_ai_hex(sql,jobs.at(i).request); }
  }
  if (OB_SUCC(ret)) { ret = trans.write(sql.ptr(),affected); }
  sql.reset();
  if (OB_SUCC(ret) && OB_SUCC(ret = sql.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu AND request_id=",jobs.at(0).table_id)) &&
      OB_SUCC(ret = append_ai_hex(sql,jobs.at(0).request)) && OB_SUCC(ret = sql.append_fmt(" AND lease_owner='%s'",owner))) {
    ret = trans.write(sql.ptr(),affected);
  }
  const int end_ret = trans.end(OB_SUCC(ret));
  if (OB_SUCC(ret)) { ret = end_ret; }
  return ret;
}


// A newly added/redefined column has no request for pre-existing rows. Seed
// those rows through native UPDATE and INSERT SELECT in one transaction. The
// generation-version marker avoids table scans triggering repeat inference.
int seed_ai_definitions(ObMySQLProxy &proxy)
{
  using namespace share::schema;
  int ret=OB_SUCCESS;
  ObSchemaGetterGuard guard;
  if(GCTX.schema_service_==NULL){return OB_NOT_INIT;}
  if(OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))){return ret;}
  ObSEArray<const ObTableSchema *,16> tables;
  if(OB_FAIL(guard.get_table_schemas_in_runtime(tables))){return ret;}
  for(int64_t t=0;OB_SUCC(ret)&&t<tables.count();++t){
    const ObTableSchema &table=*tables.at(t);
    if(!table.is_user_table()||table.is_in_recyclebin()){continue;}
    for(int64_t c=0;OB_SUCC(ret)&&c<table.get_column_count();++c){
      const ObColumnSchemaV2 *column=table.get_column_schema_by_idx(c);
      if(!column->is_ai_column()){continue;}
      const ObColumnSchemaV2 *state=table.get_column_schema(column->get_ai_state_column_id());
      const ObColumnSchemaV2 *request=table.get_column_schema(column->get_ai_request_column_id());
      const ObColumnSchemaV2 *spec=table.get_column_schema(column->get_ai_result_spec_column_id());
      const ObDatabaseSchema *db=NULL;
      if(state==NULL||request==NULL||spec==NULL){ret=OB_SCHEMA_EAGAIN;break;}
      if(OB_FAIL(guard.get_database_schema(table.get_database_id(),db))||db==NULL){break;}
      AiJob job;job.db=db->get_database_name_str();job.table=table.get_table_name_str();
      ObSqlString qualified,key_expr,sql;
      if(OB_SUCC(ret=append_ai_table(qualified,job))&&OB_SUCC(ret=key_expr.append("CONCAT("))){
        for(int64_t k=0;OB_SUCC(ret)&&k<table.get_rowkey_column_num();++k){
          const ObColumnSchemaV2 *key=table.get_column_schema(table.get_rowkey_info().get_column(k)->column_id_);
          ObSqlString prefix;
          if(k>0){ret=prefix.append(" AND ");}
          if(OB_SUCC(ret)){ret=append_ai_identifier(prefix,key->get_column_name_str());}
          if(OB_SUCC(ret)){ret=prefix.append(" = ");}
          if(k>0&&OB_SUCC(ret)){ret=key_expr.append(",");}
          if(OB_SUCC(ret)){ret=append_ai_hex(key_expr,prefix.string());}
          if(OB_SUCC(ret)){ret=key_expr.append(",");}
          if(ob_is_numeric_type(key->get_data_type())){
            if(OB_SUCC(ret)){ret=key_expr.append("CAST(");}
            if(OB_SUCC(ret)){ret=append_ai_identifier(key_expr,key->get_column_name_str());}
            if(OB_SUCC(ret)){ret=key_expr.append(" AS CHAR)");}
          }else{
            if(OB_SUCC(ret)){ret=key_expr.append("CONCAT(X'5827',HEX(CAST(");}
            if(OB_SUCC(ret)){ret=append_ai_identifier(key_expr,key->get_column_name_str());}
            if(OB_SUCC(ret)){ret=key_expr.append(" AS BINARY)),X'27')");}
          }
        }
      }
      if(OB_SUCC(ret)){ret=key_expr.append(")");}
      ObMySQLTransaction trans;int64_t affected=0;
      if(OB_SUCC(ret)){ret=trans.start(&proxy);}
      if(OB_SUCC(ret)&&OB_SUCC(ret=sql.append_fmt("UPDATE %s SET ",qualified.ptr()))&&
         OB_SUCC(ret=append_ai_identifier(sql,column->get_column_name_str()))&&
         OB_SUCC(ret=sql.append_fmt("=NULL,`%s`=0,`%s`=UUID(),`%s`='%ld' WHERE COALESCE(`%s`,'')<>'%ld'",
           state->get_column_name(),request->get_column_name(),spec->get_column_name(),column->get_ai_definition_version(),
           spec->get_column_name(),column->get_ai_definition_version()))){ret=trans.write(sql.ptr(),affected);}
      if(OB_SUCC(ret)&&affected>0){
        sql.reset();
        if(OB_SUCC(ret=sql.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu AND column_id=%lu AND definition_version<>%ld",
           table.get_table_id(),column->get_column_id(),column->get_ai_definition_version()))){ret=trans.write(sql.ptr(),affected);}
        sql.reset();
        if(OB_SUCC(ret)&&OB_SUCC(ret=sql.append_fmt(
          "INSERT INTO oceanbase.__all_ai_column_task(table_id,column_id,request_id,definition_version,rowkey_predicate,eligible_ts) "
          "SELECT %lu,%lu,d.`%s`,%ld,%s,now(6) FROM %s d WHERE d.`%s`=0 AND d.`%s`='%ld' AND NOT EXISTS "
          "(SELECT 1 FROM oceanbase.__all_ai_column_task q WHERE q.table_id=%lu AND q.column_id=%lu AND q.request_id=d.`%s`)",
          table.get_table_id(),column->get_column_id(),request->get_column_name(),column->get_ai_definition_version(),key_expr.ptr(),
          qualified.ptr(),state->get_column_name(),spec->get_column_name(),column->get_ai_definition_version(),
          table.get_table_id(),column->get_column_id(),request->get_column_name()))){ret=trans.write(sql.ptr(),affected);}
      }
      if(trans.is_started()){const int end_ret=trans.end(OB_SUCC(ret));if(OB_SUCC(ret)){ret=end_ret;}}
    }
  }
  return ret;
}

int process_ai_group(ObMySQLProxy &proxy)
{
  ObArenaAllocator alloc(ObMemAttr("AiColumnWorker"));
  ObMySQLTransaction trans;
  AiJob candidate;
  char owner[sql::UuidCommon::LENGTH_UUID+1] = {0};
  int ret = sql::ObExprUuid::gen_server_uuid(owner,sql::UuidCommon::LENGTH_UUID);
  if (OB_SUCC(ret)) { ret = trans.start(&proxy); }
  if (OB_SUCC(ret)) {
    ObMySQLProxy::MySQLResult result;
    ret = trans.read(result,"SELECT q.table_id,q.request_id FROM oceanbase.__all_ai_column_task q WHERE q.eligible_ts<=now(6) AND (q.state IN (0,3) OR NOT EXISTS(SELECT 1 FROM oceanbase.__all_ai_column_task p WHERE p.table_id=q.table_id AND p.request_id=q.request_id AND p.state IN (0,3))) AND (q.lease_until IS NULL OR q.lease_until<now(6)) ORDER BY q.eligible_ts LIMIT 1 FOR UPDATE");
    if (OB_SUCC(ret)) {
      sqlclient::ObMySQLResult *row = result.get_result();
      if (OB_SUCC(ret = row->next())) {
        int64_t table = 0;
        if (OB_SUCC(ret = row->get_int("table_id",table))) { candidate.table_id=table; }
        if (OB_SUCC(ret)) { ret = copy_ai_string(*row,"request_id",alloc,candidate.request); }
      }
    }
  }
  ObSqlString sql;
  int64_t affected = 0;
  if (OB_SUCC(ret) && OB_SUCC(ret = sql.append_fmt("UPDATE oceanbase.__all_ai_column_task SET lease_owner='%s',lease_until=date_add(now(6),INTERVAL 180 SECOND) WHERE table_id=%lu AND request_id=",owner,candidate.table_id)) &&
      OB_SUCC(ret = append_ai_hex(sql,candidate.request))) { ret = trans.write(sql.ptr(),affected); }
  if (trans.is_started()) { const int end_ret=trans.end(OB_SUCC(ret)); if (OB_SUCC(ret)) {ret=end_ret;} }
  if (ret == OB_ITER_END) { return OB_ENTRY_NOT_EXIST; }
  ObSEArray<AiJob,4> jobs;
  if (OB_SUCC(ret)) { ret = load_ai_group(proxy,candidate.table_id,candidate.request,alloc,jobs); }
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
    if ((jobs.at(i).state == 0 || jobs.at(i).state == 3) && jobs.at(i).eligible) { ret = run_ai_job(proxy,jobs.at(i),alloc,owner); }
  }
  // Reload after calls: a business update/retry/delete may have cancelled any
  // subset of this group while the external requests were in flight.
  jobs.reset();
  if (OB_SUCC(ret)) { ret = load_ai_group(proxy,candidate.table_id,candidate.request,alloc,jobs); }
  if (OB_SUCC(ret) && jobs.empty()) {
    sql.reset();
    if (OB_SUCC(ret=sql.append_fmt("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=%lu AND request_id=",candidate.table_id)) &&
        OB_SUCC(ret=append_ai_hex(sql,candidate.request)) && OB_SUCC(ret=sql.append_fmt(" AND lease_owner='%s'",owner))) {
      ret=proxy.write(sql.ptr(),affected);
    }
  } else if (OB_SUCC(ret)) { ret = publish_ai_group(proxy,jobs,owner); }
  return ret;
}
}

int ObAiService::start()
{
  recovery_state_.store(0);
  return is_inited_ ? ObThreadPool::start() : OB_NOT_INIT;
}

void ObAiService::run1()
{
  lib::set_thread_name("AiColumnWorker");
  while (!has_set_stop()) {
    int ret=OB_NOT_INIT;
    if (GCTX.sql_proxy_ != NULL && recovery_state_.load()!=2) {
      int expected=0;
      if (recovery_state_.compare_exchange_strong(expected,1)) {
        int64_t affected=0;
        ret=GCTX.sql_proxy_->write("UPDATE oceanbase.__all_ai_column_task SET lease_owner='',lease_until=NULL",affected);
        recovery_state_.store(OB_SUCC(ret)?2:0);
      }
    }
    if (GCTX.sql_proxy_ != NULL && recovery_state_.load()==2) {ret=process_ai_group(*GCTX.sql_proxy_);}
    if (ret==OB_ENTRY_NOT_EXIST) {
      static std::atomic<int64_t> last_seed{0};
      const int64_t now=ObTimeUtility::current_time();
      int64_t previous=last_seed.load();
      if(now-previous>1000000 && last_seed.compare_exchange_strong(previous,now)) {
        const int seed_ret=seed_ai_definitions(*GCTX.sql_proxy_);
        if(seed_ret!=OB_SUCCESS){LOG_WARN("AI column backfill deferred",K(seed_ret));}
      }
    }
    if(ret!=OB_SUCCESS&&ret!=OB_ENTRY_NOT_EXIST&&ret!=OB_NOT_INIT&&REACH_TIME_INTERVAL(1000000)) {
      LOG_WARN("AI column worker will retry after a database error",K(ret));
    }
    if (ret != OB_SUCCESS) { ob_usleep(100000); }
  }
}

int ObAiService::get_ai_service_guard(ObAiServiceGuard &ai_service_guard)
{
  // TODO: implement after cache support
  int ret = OB_SUCCESS;
  return ret;
}

ObAiServiceGuard::ObAiServiceGuard()
  : local_allocator_(ObMemAttr("AiServiceGuard", ObCtxIds::DEFAULT_CTX_ID))
{
}

ObAiServiceGuard::~ObAiServiceGuard()
{
}

int ObAiServiceGuard::check_access_privilege()
{
  int ret = OB_SUCCESS;
  bool has_priv = false;

  sql::ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = THIS_WORKER.get_session())) {
    // system background task thread do not have session, skip check
  } else {
    ObArenaAllocator tmp_allocator;
    share::schema::ObSchemaGetterGuard *schema_guard = session->get_cur_exec_ctx()->get_sql_ctx()->schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sql::ObAIServiceEndpointPrivUtil priv_util(*schema_guard);
      share::schema::ObSessionPrivInfo session_priv;
      uint64_t user_id = session->get_priv_user_id();
      if (user_id == OB_INVALID_ID) {
        user_id = OB_SYS_USER_ID;
      }
      if (OB_FAIL(schema_guard->get_session_priv_info(
                                                    user_id,
                                                    session->get_database_name(),
                                                    session_priv))) {
      } else if (OB_FAIL(priv_util.check_access_ai_model_priv(tmp_allocator, session_priv, has_priv))) {
      } else if (!has_priv) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("no privilege for ai model access", K(ret));
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "access ai model");
      }
    }
  }

  return ret;
}

int ObAiServiceGuard::get_ai_endpoint(const common::ObString &name, const share::ObAiModelEndpointInfo *&endpoint_info)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo *tmp_endpoint_info = nullptr;

  if (OB_FAIL(check_access_privilege())) {
  } else if (OB_ISNULL(tmp_endpoint_info = OB_NEWx(ObAiModelEndpointInfo, &local_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ObAiServiceExecutor::read_ai_endpoint(local_allocator_, name, *tmp_endpoint_info))) {
  } else {
    endpoint_info = tmp_endpoint_info;
  }
  return ret;
}

int ObAiServiceGuard::get_ai_endpoint_by_ai_model_name(const common::ObString &ai_model_name, const share::ObAiModelEndpointInfo *&endpoint_info, bool need_check)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo *tmp_endpoint_info = nullptr;

  if (need_check && OB_FAIL(check_access_privilege())) {
    LOG_WARN("failed to check access privilege", K(ret));
  } else if (OB_ISNULL(tmp_endpoint_info = OB_NEWx(ObAiModelEndpointInfo, &local_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ObAiServiceExecutor::read_ai_endpoint_by_ai_model_name(local_allocator_, ai_model_name, *tmp_endpoint_info))) {
  } else {
    endpoint_info = tmp_endpoint_info;
  }
  return ret;
}

} // namespace omt
} // namespace oceanbase
