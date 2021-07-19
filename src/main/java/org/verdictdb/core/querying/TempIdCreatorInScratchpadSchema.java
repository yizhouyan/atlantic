/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.querying;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.verdictdb.coordinator.QueryContext;
import org.apache.commons.lang3.tuple.Pair;

public class TempIdCreatorInScratchpadSchema implements IdCreator, Serializable {

  private static final long serialVersionUID = -8241890224536966759L;

  String scratchpadSchemaName;

  QueryContext context = null;

  final int serialNum = ThreadLocalRandom.current().nextInt(0, 1000000);

  static final String GLOBAL_KEYWORD = "internal_global_keyword";

  private Map<String, Integer> keywordIdentifierMap = new HashMap<>();

  public TempIdCreatorInScratchpadSchema(String scratchpadSchemaName) {
    this.scratchpadSchemaName = scratchpadSchemaName;
  }

  public TempIdCreatorInScratchpadSchema(String scratchpadSchemaName, QueryContext context) {
    this.scratchpadSchemaName = scratchpadSchemaName;
    this.context = context;
  }

  public int getSerialNumber() {
    return serialNum;
  }

  public void resetAliasNameGeneration() {
    resetAliasNameGeneration(GLOBAL_KEYWORD);
  }

  public void resetAliasNameGeneration(String keyword) {
    if (keywordIdentifierMap.containsKey(keyword)) {
      keywordIdentifierMap.put(keyword, 0);
    }
  }

  public String getScratchpadSchemaName() {
    return scratchpadSchemaName;
  }

  private synchronized int getNextId(String keyword) {
    if (!keywordIdentifierMap.containsKey(keyword)) {
      keywordIdentifierMap.put(keyword, 0);
    }
    int currentId = keywordIdentifierMap.get(keyword);
    keywordIdentifierMap.put(keyword, currentId + 1);
    return currentId;
  }

  private String generateUniqueIdentifier(String keyword) {
    int currentId = getNextId(keyword);
    String uniqueId = String.format("%d_%d", serialNum, currentId);
    return uniqueId;
  }

  String generateUniqueIdentifier() {
    return generateUniqueIdentifier(GLOBAL_KEYWORD);
  }

  @Override
  public String generateAliasName() {
    return String.format("verdictdb_alias_%s", generateUniqueIdentifier());
  }

  @Override
  public String generateAliasName(String keyword) {
    return String.format("verdictdb_%s_alias_%s", keyword, generateUniqueIdentifier(keyword));
  }

  @Override
  public int generateSerialNumber() {
    return getNextId(GLOBAL_KEYWORD);
  }

  @Override
  public Pair<String, String> generateTempTableName() {
    //    return Pair.of(scratchpadSchemaName, String.format("verdictdbtemptable_%d",
    // tempTableNameNum++));
    if (context == null) {
      return Pair.of(
          scratchpadSchemaName, String.format("verdictdbtemptable_%s", generateUniqueIdentifier()));
    } else {
      return Pair.of(
          scratchpadSchemaName,
          String.format(
              "verdictdbtemptable_%s_%d_%s",
              context.getVerdictContextId(),
              context.getExecutionSerialNumber(),
              generateUniqueIdentifier()));
    }
  }
}
