						<div class="modal-header">
							<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
							<h4 class="modal-title">{jobId}</h4>
						</div><!-- /modal-header -->
            <div class="modal-body">
              <h4>MapReduce</h4>
              <table class="table table-striped table-condensed table-bordered">
                <tbody>
                  <tr>
                    <td class="property-key"><strong>Num Maps</strong></td>
                    <td class="property-value-half">{stats.numberMaps}</td>
                    <td class="property-key"><strong>Num Reduces</strong></td>
                    <td class="property-value-half">{stats.numberReduces}</td>
                  </tr>
                  <tr>
                    <td class="property-key"><strong>Min Map Time</strong></td>
                    <td class="property-value-half">{stats.minMapTime}</td>
                    <td class="property-key"><strong>Min Reduce Time</strong></td>
                    <td class="property-value-half">{stats.minReduceTime}</td>
                  </tr>
                  <tr>
                    <td class="property-key"><strong>Max Map Time</strong></td>
                    <td class="property-value-half">{stats.maxMapTime}</td>
                    <td class="property-key"><strong>Nax Reduce Time</strong></td>
                    <td class="property-value-half">{stats.maxReduceTime}</td>
                  </tr>
                  <tr>
                    <td class="property-key"><strong>Avg Map Time</strong></td>
                    <td class="property-value-half">{stats.avgMapTime}</td>
                    <td class="property-key"><strong>Avg Reduce Time</strong></td>
                    <td class="property-value-half">{stats.avgReduceTime}</td>
                  </tr>
                </tbody>
              </table>

              <h4>I/O</h4>
              <table class="table table-striped table-condensed table-bordered">
                <tbody>
                  <tr>
                    <td class="property-key"><strong>Map Input Records</strong></td>
                    <td class="property-value-half">{stats.mapInputRecords}</td>
                    <td class="property-key"><strong>Reduce Input Records</strong></td>
                    <td class="property-value-half">{stats.reduceInputRecords}</td>
                  </tr>
                  <tr>
                    <td class="property-key"><strong>Map Output Records</strong></td>
                    <td class="property-value-half">{stats.mapOutputRecords}</td>
                    <td class="property-key"><strong>Reduce Output Records</strong></td>
                    <td class="property-value-half">{stats.reduceOutputRecords}</td>
                  </tr>
                  <tr>
                    <td class="property-key"><strong>Bytes Written</strong></td>
                    <td class="property-value-half">{stats.bytesWritten}</td>
                    <td class="property-key"><strong>HDFS Bytes Written</strong></td>
                    <td class="property-value-half">{stats.hdfsBytesWritten}</td>
                  </tr>
                  <tr>
                    <td class="property-key"><strong>Records Written</strong></td>
                    <td class="property-value-half">{stats.recordsWritten}</td>
                    <td class="property-key"><strong>SMM Spill Count</strong></td>
                    <td class="property-value-half">{stats.smmSpillCount}</td>
                  </tr>
                  <tr>
                    <td class="property-key"><strong>Proactive Spill Count Recs</strong></td>
                    <td class="property-value-half">{stats.proactiveSpillCountRecs}</td>
                    <td class="property-key"><strong>Proactive Spill Count Objects</strong></td>
                    <td class="property-value-half">{stats.proactiveSpillCountObjects}</td>
                  </tr>
                </tbody>
              </table>

              {?stats.inputStats}
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
                {#stats.inputStats}
                  <tr>
                    <td>{bytes}</td>
                    <td>{location}</td>
                    <td>{name}</td>
                    <td>{numberRecords}</td>
                  </tr>
                {/stats.inputStats}
                </tbody>
              </table>
              {/stats.inputStats}

              {?stats.outputStats}
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
                {#stats.outputStats}
                  <tr>
                    <td>{bytes}</td>
                    <td>{location}</td>
                    <td>{name}</td>
                    <td>{numberRecords}</td>
                  </tr>
                {/stats.outputStats}
                </tbody>
              </table>
              {/stats.outputStats}
            </div><!-- /.modal-body -->
            <div class="modal-footer">
              <a href="{state.trackingURL}" class="btn btn-info" target="_blank">View on JobTracker</a>
              <div type="button" class="btn btn-default" data-dismiss="modal">Close</button>
            </div>
