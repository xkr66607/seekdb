/*
 * Copyright (c) 2026 OceanBase.
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
#include <gtest/gtest.h>
#include "share/schema/ob_column_schema.h"

namespace oceanbase { namespace share { namespace schema {
using namespace common;

TEST(AiColumnSchema, owns_expression_and_deduplicates_dependencies)
{
  ObColumnSchemaV2 source, copy;
  char expression[] = "AI_COMPLETE('model', content)";
  ASSERT_EQ(OB_SUCCESS, source.set_ai_expr(ObString(expression)));
  ASSERT_EQ(OB_SUCCESS, source.set_ai_original_expr(ObString(expression)));
  source.set_ai_max_retries(3);
  source.set_ai_hidden_column_ids(20,21,22);
  ASSERT_EQ(OB_SUCCESS, source.add_ai_dependency_id(17));
  ASSERT_EQ(OB_SUCCESS, source.add_ai_dependency_id(17));
  ASSERT_EQ(OB_SUCCESS, copy.assign(source));
  source.reset();
  expression[0] = 'X';
  EXPECT_TRUE(copy.is_ai_column());
  EXPECT_EQ(ObString("AI_COMPLETE('model', content)"), copy.get_ai_expr());
  EXPECT_EQ(ObString("AI_COMPLETE('model', content)"), copy.get_ai_original_expr());
  EXPECT_EQ(3, copy.get_ai_max_retries());
  ASSERT_EQ(1, copy.get_ai_dependency_ids().count());
  EXPECT_EQ(17, copy.get_ai_dependency_ids().at(0));
  EXPECT_EQ(20, copy.get_ai_state_column_id());
  EXPECT_FALSE(source.is_ai_column());
}

TEST(AiColumnSchema, durable_metadata_round_trip)
{
  ObColumnSchemaV2 source, restored;
  ASSERT_EQ(OB_SUCCESS, source.set_ai_expr(ObString("AI_EMBED('model',body)")));
  ASSERT_EQ(OB_SUCCESS, source.set_ai_original_expr(ObString("AI_EMBED('model',body)")));
  source.set_schema_version(100);
  source.set_ai_max_retries(0);
  source.set_ai_hidden_column_ids(41,42,43);
  ASSERT_EQ(OB_SUCCESS, source.add_ai_dependency_id(18));
  ASSERT_EQ(OB_SUCCESS, source.add_ai_dependency_id(19));
  ObArenaAllocator allocator;
  const int64_t size=source.get_ai_column_info_size();
  char *buffer=static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr,buffer);
  int64_t position=0;
  ASSERT_EQ(OB_SUCCESS, source.serialize_ai_column_info(buffer,size,position));
  ASSERT_EQ(size,position);
  position=0;
  ASSERT_EQ(OB_SUCCESS, restored.deserialize_ai_column_info(buffer,size,position));
  ASSERT_EQ(size,position);
  allocator.reset();
  EXPECT_EQ(source.get_ai_expr(),restored.get_ai_expr());
  EXPECT_EQ(100,restored.get_ai_definition_version());
  EXPECT_EQ(0,restored.get_ai_max_retries());
  EXPECT_EQ(43,restored.get_ai_result_spec_column_id());
  ASSERT_EQ(2,restored.get_ai_dependency_ids().count());
  EXPECT_EQ(19,restored.get_ai_dependency_ids().at(1));
}

TEST(AiColumnSchema, unrelated_schema_changes_preserve_generation_version)
{
  ObColumnSchemaV2 column, copy;
  ASSERT_EQ(OB_SUCCESS,column.set_ai_expr(ObString("AI_COMPLETE('model',body)")));
  column.set_schema_version(100);
  column.set_schema_version(200);
  ASSERT_EQ(OB_SUCCESS,copy.assign(column));
  copy.set_schema_version(300);
  EXPECT_EQ(100,copy.get_ai_definition_version());
  EXPECT_EQ(300,copy.get_schema_version());
}

TEST(AiColumnSchema, rpc_schema_round_trip)
{
  ObColumnSchemaV2 source,restored;
  source.set_table_id(50001);
  source.set_column_id(18);
  ASSERT_EQ(OB_SUCCESS,source.set_column_name(ObString("result")));
  ASSERT_EQ(OB_SUCCESS,source.set_ai_expr(ObString("AI_COMPLETE('model',body)")));
  ASSERT_EQ(OB_SUCCESS,source.set_ai_original_expr(source.get_ai_expr()));
  source.set_schema_version(100);
  source.set_ai_hidden_column_ids(21,22,23);
  ASSERT_EQ(OB_SUCCESS,source.add_ai_dependency_id(17));
  ObArenaAllocator allocator;
  const int64_t size=source.get_serialize_size();
  char *buffer=static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr,buffer);
  int64_t position=0;
  ASSERT_EQ(OB_SUCCESS,source.serialize(buffer,size,position));
  ASSERT_EQ(size,position);
  position=0;
  ASSERT_EQ(OB_SUCCESS,restored.deserialize(buffer,size,position));
  ASSERT_EQ(size,position);
  allocator.reset();
  EXPECT_EQ(source.get_ai_expr(),restored.get_ai_expr());
  EXPECT_EQ(100,restored.get_ai_definition_version());
  EXPECT_EQ(23,restored.get_ai_result_spec_column_id());
  ASSERT_EQ(1,restored.get_ai_dependency_ids().count());
  EXPECT_EQ(17,restored.get_ai_dependency_ids().at(0));
}

TEST(AiColumnSchema, remaps_owner_dependencies_and_hidden_ids)
{
  ObColumnSchemaV2 column;
  column.set_column_id(16);
  column.set_ai_hidden_column_ids(20,21,22);
  ASSERT_EQ(OB_SUCCESS,column.set_ai_expr(ObString("AI_COMPLETE('m',body)")));
  ASSERT_EQ(OB_SUCCESS,column.add_ai_dependency_id(17));
  hash::ObHashMap<uint64_t,uint64_t> mapping;
  ASSERT_EQ(OB_SUCCESS,mapping.create(16,"AiSchemaTest"));
  const uint64_t ids[]={16,17,20,21,22};
  for(uint64_t id:ids){ASSERT_EQ(OB_SUCCESS,mapping.set_refactored(id,id+100));}
  ASSERT_EQ(OB_SUCCESS,column.convert_column_id(mapping));
  EXPECT_EQ(116,column.get_column_id());
  EXPECT_EQ(117,column.get_ai_dependency_ids().at(0));
  EXPECT_EQ(120,column.get_ai_state_column_id());
  EXPECT_EQ(121,column.get_ai_request_column_id());
  EXPECT_EQ(122,column.get_ai_result_spec_column_id());
}
}}}
