          <h3>
            Job Stats
            <div class="pull-right">
              <a href="{state.trackingURL}" class="btn btn-info btn-sm" target="_blank">View on JobTracker</a>
            </div>
          </h3>

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
