							<p>
								<button type="button" id="jobstats-back-btn" class="btn5">&lt; Back</button>
							</p>
						
							<table class="parameters">
								<thead>
									<tr>
										<th class="tb-pname">Detail</th>
										<th class="tb-pvalue">Value</th>
									</tr>
								</thead>
								<tbody>
									<tr>
										<td class="first">Features</td>
										<td>{#features}{.}{/features}</td>
									</tr>
									<tr>
										<td class="first">Aliases</td>
										<td>{#aliases}{.}{/aliases}</td>
									</tr>
								</tbody>
							</table>

							<br />
							
							<table id="node-stats" class="parameters">
								<thead>
									<tr>
										<th class="tb-pname">Job stat</th>
										<th class="tb-pvalue">Value</th>
									</tr>
								</thead>
								<tbody>
									<tr>
										<td class="first">Job ID</td>
										<td>{jobId}</td>
									</tr>
									<tr>
										<td class="first">Num Maps</td>
										<td>{metrics.numberMaps}</td>
									</tr>
									<tr>
										<td class="first">Num Reduces</td>
										<td>{metrics.numberReduces}</td>
									</tr>
									<tr>
										<td class="first">Min Map Time</td>
										<td>{metrics.minMapTime}</td>
									</tr>
									<tr>
										<td class="first">Max Map Time</td>
										<td>{metrics.maxMapTime}</td>
									</tr>
									<tr>
										<td class="first">Avg Map Time</td>
										<td>{metrics.avgMapTime}</td>
									</tr>
									<tr>
										<td class="first">Min Reduce Time</td>
										<td>{metrics.minReduceTime}</td>
									</tr>
									<tr>
										<td class="first">Max Reduce Time</td>
										<td>{metrics.maxReduceTime}</td>
									</tr>
									<tr>
										<td class="first">Avg Reduce Time</td>
										<td>{metrics.avgReduceTime}</td>
									</tr>
								</tbody>
							</table>
