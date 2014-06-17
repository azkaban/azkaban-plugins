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

$.namespace('azkaban');

azkaban.JavaViewerModel = Backbone.Model.extend({});
azkaban.JavaViewerView = Backbone.View.extend({
  events: {
  },

  initialize: function(settings) {
    this.model.bind('render', this.render, this);
    this.fetchStats();
  },

  render: function(evt) {
    var job = this.model.get('job');
    if (job == null) {
      return;
    }
    dust.render("stats", job, function(err, out) {
      $('#stats-container').html(out);
    });
  },

  fetchStats: function() {
    var requestURL = contextURL + "/javaviewer";
    var requestData = {
      "ajax": "fetchstats",
      "execid": execId,
      "jobid": jobId
    };
    var model = this.model;
    var successHandler = function(data) {
      if (data.jobs == null || data.jobs.length == 0) {
        console.log("No job data returned.");
        return;
      }

      var job = data.jobs[0];
      if (job.state.isComplete == "false") {
        job.jobState = "In Progress";
      }
      else if (job.state.isSuccessful == "true") {
        job.jobState = "Succeeded";
      }
      else {
        job.jobState = "Failed";
      }
      var jobConf = [];
      for (key in job.conf) {
        jobConf.push({"key": key, "value": job.conf[key]});
      }
      job.conf = null;
      job.conf = jobConf;

      model.set({'job': job});
      model.trigger('render');
    };
    $.get(requestURL, requestData, successHandler, 'json');
  },
});

var javaViewerModel;
var javaViewerView;

$(function() {
  javaViewerModel = new azkaban.JavaViewerModel();
  javaViewerView = new azkaban.JavaViewerView({
    el: $('#stats-container'),
    model: javaViewerModel,
  });
});
