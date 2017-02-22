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

var errorOut;
var errorBox;
function displaySuccess(message) {
  displayBox(message, "box-success-message");
}

function displayError(message) {
  displayBox(message, "box-error-message");
}

function displayBox(message, boxClass) {
  clearTimeout(errorOut);
  errorBox.html("");
  errorBox.append("<div class=\"" + boxClass + "\">" + message + "</div>");
  errorOut = setTimeout(function(){
    errorBox.find("div").fadeOut(3000, function() {
      $(this).remove();
    });
  }, 3000);
}

$(document).ready(function () {
  errorBox = $("#box-error");
});
