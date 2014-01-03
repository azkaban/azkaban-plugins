
              <div class="panel panel-default">
                <div class="panel-heading">
                  <button type="button" id="jobstats-back-btn" class="btn btn-default btn-sm">&lt; Back</button>
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
                      <td>{stats.numberMaps}</td>
                    </tr>
                    <tr>
                      <td class="first">Num Reduces</td>
                      <td>{stats.numberReduces}</td>
                    </tr>
                    <tr>
                      <td class="first">Min Map Time</td>
                      <td>{stats.minMapTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Max Map Time</td>
                      <td>{stats.maxMapTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Avg Map Time</td>
                      <td>{stats.avgMapTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Min Reduce Time</td>
                      <td>{stats.minReduceTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Max Reduce Time</td>
                      <td>{stats.maxReduceTime}</td>
                    </tr>
                    <tr>
                      <td class="first">Avg Reduce Time</td>
                      <td>{stats.avgReduceTime}</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <button type="button" id="jobstats-details-btn" class="btn btn-info btn-block">More Details &raquo;</button>
