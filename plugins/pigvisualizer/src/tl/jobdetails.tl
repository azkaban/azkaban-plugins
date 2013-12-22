						<div class="modal-header">
							<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
							<h4 class="modal-title">{jobId}</h4>
						</div><!-- /modal-header -->
            <div class="modal-body">
              <h4>MapReduce</h4>
              <table class="table table-striped table-condensed table-bordered">
                <tbody>
                  <tr>
                    <td><strong>Num Maps</strong></td>
                    <td>{stats.numberMaps}</td>
                    <td><strong>Num Reduces</strong></td>
                    <td>{stats.numberReduces}</td>
                  </tr>
                  <tr>
                    <td><strong>Min Map Time</strong></td>
                    <td>{stats.minMapTime}</td>
                    <td><strong>Min Reduce Time</strong></td>
                    <td>{stats.minReduceTime}</td>
                  </tr>
                  <tr>
                    <td><strong>Max Map Time</strong></td>
                    <td>{stats.maxMapTime}</td>
                    <td><strong>Nax Reduce Time</strong></td>
                    <td>{stats.maxReduceTime}</td>
                  </tr>
                  <tr>
                    <td><strong>Avg Map Time</strong></td>
                    <td>{stats.avgMapTime}</td>
                    <td><strong>Avg Reduce Time</strong></td>
                    <td>{stats.avgReduceTime}</td>
                  </tr>
                </tbody>
              </table>

              <h4>I/O</h4>
              <table class="table table-striped table-condensed table-bordered">
                <tbody>
                  <tr>
                    <td><strong>Map Input Records</strong></td>
                    <td>{stats.mapInputRecords}</td>
                    <td><strong>Reduce Input Records</strong></td>
                    <td>{stats.reduceInputRecords}</td>
                  </tr>
                  <tr>
                    <td><strong>Map Output Records</strong></td>
                    <td>{stats.mapOutputRecords}</td>
                    <td><strong>Reduce Output Records</strong></td>
                    <td>{stats.reduceOutputRecords}</td>
                  </tr>
                  <tr>
                    <td><strong>Bytes Written</strong></td>
                    <td>{stats.bytesWritten}</td>
                    <td><strong>HDFS Bytes Written</strong></td>
                    <td>{stats.hdfsBytesWritten}</td>
                  </tr>
                  <tr>
                    <td><strong>Records Written</strong></td>
                    <td>{stats.recordsWritten}</td>
                    <td><strong>SMM Spill Count</strong></td>
                    <td>{stats.smmSpillCount}</td>
                  </tr>
                  <tr>
                    <td><strong>Proactive Spill Count Recs</strong></td>
                    <td>{stats.proactiveSpillCountRecs}</td>
                    <td><strong>Proactive Spill Count Objects</strong></td>
                    <td>{stats.proactiveSpillCountObjects}</td>
                  </tr>
                </tbody>
              </table>
            </div>
