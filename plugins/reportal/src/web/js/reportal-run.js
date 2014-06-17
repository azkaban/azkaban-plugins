/*
 * Copyright 2012 LinkedIn Corp.
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

// Sends a POST request to the Reportal servlet that starts an execution of a report.
function runReport(event, buttonId, isTestRun) {
  event.preventDefault();
  $.ajax({
    url: contextURL + "/reportal?ajax=run" + (isTestRun ? "&testRun" : ""),
    type: "POST",
    data: $("form").serialize(),
    dataType: "json"
  }).done(function(data) {
    if (data.result == "success") {
      displaySuccess(data.message + " Redirecting in 5 seconds");
      setTimeout(function(){window.location.href = data.redirect;}, 5000);
    }
    else {
      displayError(data.error);
    }
  });
}

$(document).ready(function () {
  $("#run-button").click(function(event) {
    runReport(event, "#run-button", false);
  });

  $("#test-run-button").click(function(event) {
    runReport(event, "#test-run-button", true);
  });
});
