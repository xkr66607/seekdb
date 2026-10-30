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
#include "sql/parser/ob_parser.h"

namespace oceanbase { namespace sql {
using namespace common;
static const ParseNode *find_ai_node(const ParseNode *node, ObItemType type)
{
  if(node==nullptr||node->type_==type){return node;}
  for(int64_t i=0;i<node->num_child_;++i){
    const ParseNode *found=find_ai_node(node->children_[i],type);
    if(found!=nullptr){return found;}
  }
  return nullptr;
}
TEST(AiColumnParser, create_and_alter_preserve_expression)
{
  const char *statements[]={
    "CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,result JSON AI COLUMN (AI_COMPLETE('m',concat('x,',body))) WITH(sync_mode='async',max_retries=2))",
    "ALTER TABLE docs ADD COLUMN emb VECTOR(4) AI COLUMN (AI_EMBED('m',body,4))",
    "ALTER TABLE docs MODIFY COLUMN result JSON AI COLUMN (AI_COMPLETE('m',body)) WITH(max_retries=0)"};
  for(const char *sql:statements){
    ObArenaAllocator allocator;ObParser parser(allocator,0);ParseResult result;
    ASSERT_EQ(OB_SUCCESS,parser.parse(ObString(sql),result));
    const ParseNode *ai=find_ai_node(result.result_tree_,T_AI_COLUMN);
    ASSERT_NE(nullptr,ai);
    ASSERT_EQ(2,ai->num_child_);
    ASSERT_NE(nullptr,ai->children_[0]);
    EXPECT_GT(ai->children_[0]->str_len_,0);
  }
}
TEST(AiColumnParser, retry_retains_quoted_status_and_condition)
{
  ObArenaAllocator allocator;ObParser parser(allocator,0);ParseResult result;
  ASSERT_EQ(OB_SUCCESS,parser.parse(ObString("ALTER TABLE docs RETRY AI COLUMN result WHERE status='FAILED' AND `status`='active' AND id IN(1,3)"),result));
  const ParseNode *retry=find_ai_node(result.result_tree_,T_AI_COLUMN_RETRY);
  ASSERT_NE(nullptr,retry);
  ASSERT_EQ(2,retry->num_child_);
  EXPECT_EQ(ObString("result"),ObString(retry->children_[0]->str_len_,retry->children_[0]->str_value_));
  ASSERT_NE(nullptr,retry->children_[1]);
  EXPECT_GT(retry->children_[1]->str_len_,0);
}
TEST(AiColumnParser, show_ai_columns)
{
  ObArenaAllocator allocator;ObParser parser(allocator,0);ParseResult result;
  ASSERT_EQ(OB_SUCCESS,parser.parse(ObString("SHOW AI COLUMNS FROM docs"),result));
  const ParseNode *show=find_ai_node(result.result_tree_,T_SHOW_COLUMNS);
  ASSERT_NE(nullptr,show);
  EXPECT_EQ(4,show->children_[0]->value_);
}
}}
