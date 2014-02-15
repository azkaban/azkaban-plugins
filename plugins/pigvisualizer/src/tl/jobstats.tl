
              <div class="panel panel-default">
                <div class="panel-heading">
                  <button type="button" id="jobstats-back-btn" class="btn btn-default btn-sm"><span class="glyphicon glyphicon-chevron-left"></span> Back</button>
                </div>
                <table class="table table-condensed table-striped table-hover table-bordered">
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
                    <tr>
                      <td class="first">State</td>
                      <td>{jobState}</td>
                    </tr>
                    <tr>
                      <td class="first">JobTracker URL</td>
                      <td><a href="{state.trackingURL}" class="btn btn-info btn-xs" target="blank">View on JobTracker</a></td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <div class="panel panel-default">
                <div class="panel-heading">Counters</div>
                <table id="node-stats" class="table table-condensed table-striped table-hover table-bordered">
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
                      <td>{pigStats.numberMaps}</td>
                    </tr>
                    <tr>
                      <td class="first">Num Reduces</td>
                      <td>{pigStats.numberReduces}</td>
                    </tr>
                    <tr>
                      <td class="first">Min Map Time</td>
                      <td>{pigStats.minMapTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Max Map Time</td>
                      <td>{pigStats.maxMapTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Avg Map Time</td>
                      <td>{pigStats.avgMapTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Min Reduce Time</td>
                      <td>{pigStats.minReduceTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Max Reduce Time</td>
                      <td>{pigStats.maxReduceTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Avg Reduce Time</td>
                      <td>{pigStats.avgReduceTime}</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <button type="button" id="jobstats-details-btn" class="btn btn-info btn-block">More Details &raquo;</button>
