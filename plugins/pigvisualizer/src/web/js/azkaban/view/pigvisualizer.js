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

var visualizerGraphModel;
azkaban.VisualizerGraphModel = Backbone.Model.extend({});

var nodeClickCallback = function (event, model, type) {
	console.log("Node clicked callback");
	var nodeId = event.currentTarget.jobid;
	var jobRequestURL = contextURL + "/pigvisualizer?execid=" + execId + 
			"&jobid=" + jobId + "&nodeId=" + nodeId;
	var menu = [
		{
			title: "Open Node...", 
			callback: function() { 
				window.location.href = jobRequestURL; 
			}
		},
		{
			title: "Open Node in New Window...", 
			callback: function() { 
				window.open(jobRequestURL); 
			}
		},
		{break: 1},
		{
			title: "Center Job", 
			callback: function() {
				model.trigger("centerNode", nodeId); 
			}
		}
	];
	contextMenuView.show(event, menu);
}

var jobClickCallback = function (event, model) {
	console.log("Job clicked callback");
	var nodeId = event.currentTarget.jobid;
	var jobRequestURL = contextURL + "/pigvisualizer?execid=" + execId + 
			"&jobid=" + jobId + "&nodeId=" + nodeId;
	var menu = [
		{
			title: "Open Job...", 
			callback: function() { 
				window.location.href = jobRequestURL; 
			}
		},
		{
			title: "Open Job in New Window...", 
			callback: function() { 
				window.open(jobRequestURL);
			}
		},
		{break: 1},
		{
			title: "Center Job", 
			callback: function() { 
				model.trigger("centerNode", nodeId);
			}
		}
	];
	contextMenuView.show(event, menu);
}

var edgeClickCallback = function (event, model) {
	console.log("Edge clicked callback");
}

var graphClickCallback = function (event, model) {
	console.log("Graph clicked callback");
	var menu = [
		{
			title: "Center graph", 
			callback: function() { 
				model.trigger("resetPanZoom"); 
			}
		}
	];
	contextMenuView.show(event, menu);
}

var jobDetailsView;
azkaban.JobDetailsView = Backbone.View.extend({
	events: {
    "click #details-tab": "handleDetailsTabClick",
    "click #jobconf-tab": "handleJobConfTabClick"
  },
	
  initialize: function (settings) {
  },
  
  handleDetailsTabClick: function() {
    console.log("handle details tab click");
    $('#jobconf-tab').removeClass('active');
    $('#details-tab').addClass('active');
    $('#jobconf-tab-pane').hide();
    $('#details-tab-pane').show();
  },

  handleJobConfTabClick: function() {
    console.log("handle details tab click");
    $('#details-tab').removeClass('active');
    $('#jobconf-tab').addClass('active');
    $('#details-tab-pane').hide();
    $('#jobconf-tab-pane').show();
  },

  render: function() {

  }
});

var jobStatsView;
azkaban.JobStatsView = Backbone.View.extend({
	events: {
		"click .job": "handleJobClick",
		"click .resetPanZoomBtn": "handleResetPanZoom",
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
		if (!evt.currentTarget.jobid) {
			return;
		}
		var jobid = evt.currentTarget.jobid;

		if (this.model.has("selected")) {
			var selected = this.model.get("selected");
			if (selected == jobid) {
				this.model.unset("selected");
			}
			else {
				this.model.set({"selected": jobid});
			}
		}
		else {
			this.model.set({"selected": jobid});
		}
	},

	handleBackButton: function (evt) {
    console.log("handleBackButton");
		this.model.unset("selected");
		$('#jobstats-details').hide();
		$('#jobstats-list').show();
	},

	handleSelectionChange: function (evt) {
		if (!this.model.hasChanged("selected") || !this.model.has("selected")) {
			return;
		}

		var previous = this.model.previous("selected");
		var current = this.model.get("selected");

		var nodes = this.model.get("nodes");
		var node = nodes[current];
		if (node.detailsProcessed == true) {
			this.renderSidebar(current);
			return;
		}

		if (node.state.isComplete == "false") {
			node.jobState = "In Progress";
		}
		else if (node.state.isSuccessful == "true") {
			node.jobState = "Succeeded";
		}
		else {
			node.jobState = "Failed";
		}
		var jobConf = [];
		for (key in node.conf) {
			jobConf.push({"key": key, "value": node.conf[key]});
		}
		node.conf = null;
		node.conf = jobConf;

		this.renderSidebar(current);
	},

  renderSidebar: function(nodeId) {
		var nodes = this.model.get("nodes");
		var data = nodes[nodeId];
    if (data == null) {
      return;
    }
    dust.render("jobstats", data, function (err, out) {
      $('#jobstats-list').hide();
      $('#jobstats-details').show();
      $('#jobstats-details').html(out);
    });
    dust.render("jobdetails", data, function (err, out) {
      $('#job-details-modal-content').html(out);
      $('#job-details-modal').on('shown.bs.modal', function(e) {
        jobDetailsView.handleDetailsTabClick();
      });
    });
  },

  handleJobDetailsModal: function(evt) {
		var current = this.model.get("selected");
		var nodes = this.model.get("nodes");
		var data = nodes[current];
    if (data == null) {
      return;
		}
    $('#job-details-modal').modal();
  },


	handleResetPanZoom: function (evt) {
		this.model.trigger("resetPanZoom");
	},

	render: function (self) {
		$('#jobstats-details').hide();
		var data = this.model.get("data");
		var nodes = data.nodes;
		var edges = data.edges;

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
			$(a).text(nodeArray[i].id);
			$(list).append(a);
			a.jobid = nodeArray[i].id;
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

$(function() {
	visualizerGraphModel = new azkaban.VisualizerGraphModel();
	visualizerGraphView = new azkaban.VisualizerGraphView({
		el: $('#svgDiv'),
		model: visualizerGraphModel,
		rightClick: {
			"node": nodeClickCallback,
			"edge": edgeClickCallback,
			"graph": graphClickCallback
		}
	});

	jobStatsView = new azkaban.JobStatsView({
		el: $('#jobStats'),
		model: visualizerGraphModel,
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
		var nodes = [];
		var edges = [];
		var jobs = data.jobs;
		for (var i = 0; i < jobs.length; ++i) {
			var job = jobs[i];
			job.id = job.name;
			job.level = parseInt(job.level);
			nodes.push(job);
			for (var j = 0; j < job.successors.length; ++j) {
				var edge = {
					from: job.name,
					target: job.successors[j]
				};
				edges.push(edge);
			}
		}
		data.nodes = nodes;
		data.edges = edges;

		createModelFromAjaxCall(data, visualizerGraphModel);
		visualizerGraphModel.trigger("change:graph");
	};

	$.get(requestURL, request, successHandler, "json");
});
