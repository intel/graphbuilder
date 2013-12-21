/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
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
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;

import org.apache.log4j.Logger;

public class GraphBuilderExit {

    public static void graphbuilderFatalExitException(StatusCode statusCode, String message, Logger log, Exception e) {
        log.fatal(message);
        System.err.println(message);  // two places? hey why not, make this stuff easy to find
        System.err.println(statusCode.getMessage());
        System.err.println(e.getMessage());
        e.printStackTrace(System.err);
        System.exit(statusCode.getStatus());
    }

    public static void graphbuilderFatalExitNoException(StatusCode statusCode, String message, Logger log) {
        log.fatal(message);
        System.err.println(message);  // two places? hey why not, make this stuff easy to find
        System.err.println(statusCode.getMessage());
        System.exit(statusCode.getStatus());
    }

    public static void graphbuilderExitNoException(StatusCode statusCode) {
        System.exit(statusCode.getStatus());
    }
}