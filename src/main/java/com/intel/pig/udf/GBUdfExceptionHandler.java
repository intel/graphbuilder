/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */
package com.intel.pig.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.MonitoredUDFExecutor.ErrorCallback;

import java.io.IOException;

/**
 * GBUdfExceptionHandler is an exception handler used for handling exceptions
 * thrown from the UDFs unless their cause are of type {@link GBUdfException}.
 * This class simply logs the error & ensures that processing goes on.
 * 
 */
public class GBUdfExceptionHandler extends ErrorCallback {
	private static final Log log = LogFactory
			.getLog(GBUdfExceptionHandler.class);

	@SuppressWarnings("unchecked")
	public static void handleError(EvalFunc evalFunc, Exception e) {
		if (e instanceof IOException) {
			Throwable cause = e.getCause();
			if (cause != null && cause instanceof GBUdfException) {
				/* bubble up custom exceptions */
				throw new RuntimeException(e);
			}
		}
		log.error("Caught unexpected exception." + evalFunc + " failed.", e);
	}
}
