package hs.jjm;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Similar interface as {@link org.apache.hadoop.util.Progressable} but returns a boolean to support
 * canceling the operation.
 * <p/>
 * Used for doing updating of OPENING znode during log replay on region open.
 */
@InterfaceAudience.Private
public interface CancelableProgressable {

  /**
   * Report progress. Returns true if operations should continue, false if the operation should be
   * canceled and rolled back.
   * @return whether to continue (true) or cancel (false) the operation
   */
  boolean progress();

}
