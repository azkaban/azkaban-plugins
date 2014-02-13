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

var jobDetailsView;
azkaban.JobDetailsView = Backbone.View.extend({
	events: {
    "click #details-tab": "handleDetailsTabClick",
    "click #counters-tab": "handleCountersTabClick",
    "click #jobconf-tab": "handleJobConfTabClick"
  },

  initialize: function (settings) {
  },

  handleDetailsTabClick: function() {
    $('#jobconf-tab').removeClass('active');
    $('#counters-tab').removeClass('active');
    $('#details-tab').addClass('active');
    $('#counters-tab-pane').hide();
    $('#jobconf-tab-pane').hide();
    $('#details-tab-pane').show();
  },

  handleJobConfTabClick: function() {
    $('#details-tab').removeClass('active');
    $('#counters-tab').removeClass('active');
    $('#jobconf-tab').addClass('active');
    $('#details-tab-pane').hide();
    $('#counters-tab-pane').hide();
    $('#jobconf-tab-pane').show();
  },

  handleCountersTabClick: function() {
    $('#details-tab').removeClass('active');
    $('#jobconf-tab').removeClass('active');
    $('#counters-tab').addClass('active');
    $('#details-tab-pane').hide();
    $('#jobconf-tab-pane').hide();
    $('#counters-tab-pane').show();
  },

  render: function() {

  }
});

var jobStatsView;
azkaban.JobStatsView = Backbone.View.extend({
	events: {
		"click .job": "handleJobClick",
		"click #resetPanZoomBtn": "handleResetPanZoom",
		"click #autoPanZoomBtn": "handleAutoPanZoom",
		"click #jobstats-back-btn": "handleBackButton",
		"contextMenu li": "handleContextMenuClick",
    "click #jobstats-details-btn": "handleJobDetailsModal",
	},

	initialize: function (settings) {
		this.model.bind('change:selected', this.handleSelectionChange, this);
		this.model.bind('change:graph', this.render, this);

		this.contextMenu = settings.contextMenuCallback;
		this.listNodes = {};
		this.list = $(this.el).find("#list");
	},

	handleJobClick: function (evt) {
		if (!evt.currentTarget.data) {
			return;
		}
		var node = evt.currentTarget.data;

		if (this.model.has("selected")) {
			var selected = this.model.get("selected");
			if (selected == node) {
				this.model.unset("selected");
			}
			else {
				this.model.set({"selected": node});
			}
		}
		else {
			this.model.set({"selected": node});
		}
	},

	handleBackButton: function (evt) {
    console.log("handleBackButton");
		this.model.unset("selected");
		$('#jobstats-details').hide();
		$('#jobstats-list').show();
	},

  reformatConf: function (conf) {
    if (conf == null) {
      return null;
    }
		var jobConf = [];
		for (var key in conf) {
			jobConf.push({
        key: key,
        value: conf[key]
      });
		}
    return jobConf;
  },

  reformatCounters: function (rawCounters) {
    if (rawCounters == null) {
      return null;
    }
    var jobCounters = [];
    for (var counterName in rawCounters) {
      var counterGroup = {
        name: counterName,
        counters: []
      };
      var counters = rawCounters[counterName];
      for (var key in counters) {
        counterGroup.counters.push({
          key: key,
          value: counters[key]
        });
      }
      jobCounters.push(counterGroup);
    }
    return jobCounters;
  },

	handleSelectionChange: function (evt) {
		if (!this.model.hasChanged("selected") || !this.model.has("selected")) {
			return;
		}

		var previous = this.model.previous("selected");
		var current = this.model.get("selected");
		if (current.clicked == true) {
			this.renderSidebar(current);
			return;
		}

		if (current.state.isComplete == "false") {
			current.jobState = "In Progress";
		}
		else if (current.state.isSuccessful == "true") {
			current.jobState = "Succeeded";
		}
		else {
			current.jobState = "Failed";
		}
		current.conf = this.reformatConf(current.conf);
    current.counterGroups = this.reformatCounters(current.state.counters);
		current.clicked = true;

		this.renderSidebar(current);
	},

  renderSidebar: function(node) {
    if (node == null) {
      return;
    }
    dust.render("jobstats", node, function (err, out) {
      $('#jobstats-list').hide();
      $('#jobstats-details').show();
      $('#jobstats-details').html(out);
    });
    dust.render("jobdetails", node, function (err, out) {
      $('#job-details-modal-content').html(out);
      $('#job-details-modal').on('shown.bs.modal', function(e) {
        jobDetailsView.handleDetailsTabClick();
      });
    });
  },

  handleJobDetailsModal: function(evt) {
		var current = this.model.get("selected");
    if (current == null) {
      return;
		}
    $('#job-details-modal').modal();
  },

	handleAutoPanZoom: function(evt) {
		var target = evt.currentTarget;
		if ($(target).hasClass('btn-default')) {
			$(target).removeClass('btn-default');
			$(target).addClass('btn-info');
		}
		else if ($(target).hasClass('btn-info')) {
			$(target).removeClass('btn-info');
			$(target).addClass('btn-default');
		}

		// Using $().hasClass('active') does not use here because it appears that
		// this is called before the Bootstrap toggle completes.
		this.model.set({"autoPanZoom": $(target).hasClass('btn-info')});
	},

	handleResetPanZoom: function (evt) {
		this.model.trigger("resetPanZoom");
	},

	render: function (self) {
		$('#jobstats-details').hide();
		var data = this.model.get("data");
		var nodes = data.nodes;

		this.listNodes = {};
		if (nodes.length == 0) {
			console.log("No results");
			return;
		}

		var nodeArray = nodes.slice(0);
		nodeArray.sort(function(a, b) {
			var diff = a.y - b.y;
			if (diff == 0) {
				return a.x - b.x;
			}
			else {
				return diff;
			}
		});

		var list = this.list;
		this.jobs = list;
		for (var i = 0; i < nodeArray.length; ++i) {
			var a = document.createElement("a");
      $(a).addClass("list-group-item");
      $(a).addClass("job");
      $(a).attr('href', '#');
			$(a).text(nodeArray[i].aliases.join());
			$(list).append(a);
			a.data = nodeArray[i];
			this.listNodes[nodeArray[i].id] = a;
		}
	},

	handleContextMenuClick: function(evt) {
		if (this.contextMenu) {
			this.contextMenu(evt);
			return false;
		}
	}
});

var contextMenuView;
var graphView;
var graphModel;

$(function() {
  $('#graphView').hide();
	graphModel = new azkaban.GraphModel();
	graphView = new azkaban.SvgGraphView({
		el: $('#svgDiv'),
		model: graphModel,
		rightClick: {
			"node": nodeClickCallback,
			"edge": edgeClickCallback,
			"graph": graphClickCallback
		}
	});

	jobStatsView = new azkaban.JobStatsView({
		el: $('#jobStats'),
		model: graphModel,
		contextMenuCallback: jobClickCallback
	});

  jobDetailsView = new azkaban.JobDetailsView({
    el: $('#job-details-modal'),
  });

	var requestURL = contextURL + "/pigvisualizer";
	var request = {
		"ajax": "fetchjobs",
		"execid": execId,
		"jobid": jobId
	};

	var successHandler = function (data) {
    if (data == null || data.jobs == null || data.jobs.length == 0) {
      return;
    }

    $('#placeholder').hide();
    $('#graphView').show();

		var nodes = [];
		var jobs = data.jobs;
		for (var i = 0; i < jobs.length; ++i) {
			var job = jobs[i];
			job.id = job.name;
			job.level = parseInt(job.level);
			job.in = job.parents;
			job.type = "job";
			job.clicked = false;
			nodes.push(job);
		}
		data.nodes = nodes;

		graphModel.addFlow(data);
		graphModel.trigger("change:graph");
	};

	$.get(requestURL, request, successHandler, "json");
});
