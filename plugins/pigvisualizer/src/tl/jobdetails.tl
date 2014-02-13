            <div class="modal-header">
              <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
              <h4 class="modal-title">{jobId}</h4>
            </div><!-- /modal-header -->
            <div class="modal-body">
              <ul class="nav nav-tabs">
                <li id="details-tab"><a href="#">Details</a></li>
                <li id="counters-tab"><a href="#">Counters</a></li>
                <li id="jobconf-tab"><a href="#">Configuration</a></li>
              </ul>

              <div id="details-tab-pane">
                <h4>MapReduce</h4>
                <table class="table table-striped table-condensed table-bordered">
                  <tbody>
                    <tr>
                      <td class="property-key"><strong>Num Maps</strong></td>
                      <td class="property-value-half">{pigStats.numberMaps}</td>
                      <td class="property-key"><strong>Num Reduces</strong></td>
                      <td class="property-value-half">{pigStats.numberReduces}</td>
                    </tr>
                    <tr>
                      <td class="property-key"><strong>Min Map Time</strong></td>
                      <td class="property-value-half">{pigStats.minMapTime}</td>
                      <td class="property-key"><strong>Min Reduce Time</strong></td>
                      <td class="property-value-half">{pigStats.minReduceTime}</td>
                    </tr>
                    <tr>
                      <td class="property-key"><strong>Max Map Time</strong></td>
                      <td class="property-value-half">{pigStats.maxMapTime}</td>
                      <td class="property-key"><strong>Nax Reduce Time</strong></td>
                      <td class="property-value-half">{pigStats.maxReduceTime}</td>
                    </tr>
                    <tr>
                      <td class="property-key"><strong>Avg Map Time</strong></td>
                      <td class="property-value-half">{pigStats.avgMapTime}</td>
                      <td class="property-key"><strong>Avg Reduce Time</strong></td>
                      <td class="property-value-half">{pigStats.avgReduceTime}</td>
                    </tr>
                  </tbody>
                </table>

                <h4>I/O</h4>
                <table class="table table-striped table-condensed table-bordered">
                  <tbody>
                    <tr>
                      <td class="property-key"><strong>Map Input Records</strong></td>
                      <td class="property-value-half">{pigStats.mapInputRecords}</td>
                      <td class="property-key"><strong>Reduce Input Records</strong></td>
                      <td class="property-value-half">{pigStats.reduceInputRecords}</td>
                    </tr>
                    <tr>
                      <td class="property-key"><strong>Map Output Records</strong></td>
                      <td class="property-value-half">{pigStats.mapOutputRecords}</td>
                      <td class="property-key"><strong>Reduce Output Records</strong></td>
                      <td class="property-value-half">{pigStats.reduceOutputRecords}</td>
                    </tr>
                    <tr>
                      <td class="property-key"><strong>Bytes Written</strong></td>
                      <td class="property-value-half">{pigStats.bytesWritten}</td>
                      <td class="property-key"><strong>HDFS Bytes Written</strong></td>
                      <td class="property-value-half">{pigStats.hdfsBytesWritten}</td>
                    </tr>
                    <tr>
                      <td class="property-key"><strong>Records Written</strong></td>
                      <td class="property-value-half">{pigStats.recordsWritten}</td>
                      <td class="property-key"><strong>SMM Spill Count</strong></td>
                      <td class="property-value-half">{pigStats.smmSpillCount}</td>
                    </tr>
                    <tr>
                      <td class="property-key"><strong>Proactive Spill Count Recs</strong></td>
                      <td class="property-value-half">{pigStats.proactiveSpillCountRecs}</td>
                      <td class="property-key"><strong>Proactive Spill Count Objects</strong></td>
                      <td class="property-value-half">{pigStats.proactiveSpillCountObjects}</td>
                    </tr>
                  </tbody>
                </table>

                {?pigStats.inputStats}
                <h4>Inputs</h4>
                <table class="table table-striped table-condensed table-bordered">
                  <thead>
                    <tr>
                      <th>Bytes</th>
                      <th>Location</th>
                      <th>Name</th>
                      <th>Num Records</th>
                    </tr>
                  </thead>
                  <tbody>
                  {#pigStats.inputStats}
                    <tr>
                      <td>{bytes}</td>
                      <td>{location}</td>
                      <td>{name}</td>
                      <td>{numberRecords}</td>
                    </tr>
                  {/pigStats.inputStats}
                  </tbody>
                </table>
                {/pigStats.inputStats}

                {?pigStats.outputStats}
                <h4>Outputs</h4>
                <table class="table table-striped table-condensed table-bordered">
                  <thead>
                    <tr>
                      <th>Bytes</th>
                      <th>Location</th>
                      <th>Name</th>
                      <th>Num Records</th>
                    </tr>
                  </thead>
                  <tbody>
                  {#pigStats.outputStats}
                    <tr>
                      <td>{bytes}</td>
                      <td>{location}</td>
                      <td>{name}</td>
                      <td>{numberRecords}</td>
                    </tr>
                  {/pigStats.outputStats}
                  </tbody>
                </table>
                {/pigStats.outputStats}
              </div>

              <div id="counters-tab-pane">
                {#counterGroups}
                  <h4>{name}</h4>
                  <table class="table table-striped table-condensed table-bordered">
                    <thead>
                      <tr>
                        <th>Counter</th>
                        <th>Total</th>
                      </tr>
                    </thead>
                    <tbody>
                    {#counters}
                      <tr>
                        <td class="property-key">{key}</td>
                        <td>{value}</td>
                      </tr>
                    {/counters}
                    </tbody>
                  </table>
                {/counterGroups}
              </div>

              <div id="jobconf-tab-pane">
                <h4>Job Configuration</h4>
                <table class="table table-striped table-condensed table-bordered table-properties">
                  <tbody>
                  {#conf}
                    <tr>
                      <td class="property-key"><pre>{key}</pre></td>
                      <td class="property-value"><pre>{value}</pre></td>
                    </tr>
                  {/conf}
                  </tbody>
                </table>
                <div class="callout callout-info">
                  <h4>Full Job Conf</h4>
                  <p>For the full job configuration, please view this job on the JobTracker.</p>
                </div>
              </div>
            </div><!-- /.modal-body -->
            <div class="modal-footer">
              <a href="{state.trackingURL}" class="btn btn-info" target="_blank">View on JobTracker</a>
              <div type="button" class="btn btn-default" data-dismiss="modal">Close</button>
            </div>
