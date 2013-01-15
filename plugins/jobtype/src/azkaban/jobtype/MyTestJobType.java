
/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype;


import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import azkaban.utils.Props;

public class MyTestJobType extends ProcessJob {

	public static final String COMMAND = "command";
    private static final long KILL_TIME_MS = 5000;
    private volatile AzkabanProcess process;

	public MyTestJobType(final String jobId, final Props sysProps, final Props jobProps, final Logger log) {
		super(jobId, sysProps, jobProps, log);
	}

	@Override
	public void run() throws Exception {
		getLog().info("Running my custom job type");
		super.run();
		getLog().info("Finished my custom job type");
	}


	protected List<String> getCommandList() {
		List<String> commands = new ArrayList<String>();
		commands.add(getProps().getString(COMMAND));
		for (int i = 1; getProps().containsKey(COMMAND + "." + i); i++) {
			commands.add(getProps().getString(COMMAND + "." + i));
		}

		return commands;
	}

    @Override
    public void cancel() throws InterruptedException {
        if(process == null)
            throw new IllegalStateException("Not started.");
        boolean killed = process.softKill(KILL_TIME_MS, TimeUnit.MILLISECONDS);
        if(!killed) {
            warn("Kill with signal TERM failed. Killing with KILL signal.");
            process.hardKill();
        }
    }

    @Override
    public double getProgress() {
        return process != null && process.isComplete()? 1.0 : 0.0;
    }
    
	public int getProcessId() {
		return process.getProcessId();
	}

	@Override
	public Props getProps() {
		return jobProps;
	}

	public String getPath() {
		return _jobPath == null ? "" : _jobPath;
	}

	public String getJobName() {
		return getId();
	}

	/**
	 * Splits the command into a unix like command line structure. Quotes and
	 * single quotes are treated as nested strings.
	 * 
	 * @param command
	 * @return
	 */
	public static String[] partitionCommandLine(final String command) {
		ArrayList<String> commands = new ArrayList<String>();

		int index = 0;

		StringBuffer buffer = new StringBuffer(command.length());

		boolean isApos = false;
		boolean isQuote = false;
		while (index < command.length()) {
			char c = command.charAt(index);

			switch (c) {
			case ' ':
				if (!isQuote && !isApos) {
					String arg = buffer.toString();
					buffer = new StringBuffer(command.length() - index);
					if (arg.length() > 0) {
						commands.add(arg);
					}
				} else {
					buffer.append(c);
				}
				break;
			case '\'':
				if (!isQuote) {
					isApos = !isApos;
				} else {
					buffer.append(c);
				}
				break;
			case '"':
				if (!isApos) {
					isQuote = !isQuote;
				} else {
					buffer.append(c);
				}
				break;
			default:
				buffer.append(c);
			}

			index++;
		}

		if (buffer.length() > 0) {
			String arg = buffer.toString();
			commands.add(arg);
		}

		return commands.toArray(new String[commands.size()]);
	}
}