/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package usingflume.ch03;

import java.util.List;
import java.util.Map;

public interface StockServer {

  void start();

  void stop();
  /**
   * Returns a map of ticker to its current price.
   * @param tickers - List of tickers whose prices must be fetched.
   * @return - a map of ticker to its current price
   */
  Map<String, Float> getStockPrice(List<String> tickers);
}
