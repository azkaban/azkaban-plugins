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

var svgGraphView;
var graphModel;
azkaban.GraphModel = Backbone.Model.extend({});

var mainSvgGraphView;
//var jobStatsView;
var contextMenuView;

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

/*azkaban.JobStatsView = Backbone.View.extend({
	events: {
		"click li": "handleJobClick",
		"click .resetPanZoomBtn": "handleResetPanZoom",
		"contextMenu li": "handleContextMenuClick"
	},

	initialize: function (settings) {
		this.model.bind('change:selected', this.handleSelectionChange, this);
		this.model.bind('change:graph', this.render, this);

		this.contextMenu = settings.contextMenuCallback;
		this.listNodes = {};
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

	handleSelectionChange: function (evt) {
		if (!this.model.hasChanged("selected")) {
			return;
		}
		var previous = this.model.previous("selected");
		var current = this.model.get("selected");

		// XXX Update sidebar.
	},

	handleResetPanZoom: function (evt) {
		this.model.trigget("resetPanZoom");
	},

	render: function (self) {

	}
});*/

$(function() {
	graphModel = new azkaban.GraphModel();
	mainSvgGraphView = new azkaban.SvgGraphView({
		el: $('#svgDiv'),
		model: graphModel,
		rightClick: {
			"node": nodeClickCallback,
			"edge": edgeClickCallback,
			"graph": graphClickCallback
		}
	});

	/*jobStatsView = new azkaban.JobStatsView({
		el: $('#jobStats'),
		model: graphModel,
		contextMenuCallback: jobClickCallback
	});*/

	var requestURL = contextURL + "/pigvisualizer";
	var request = {
		"ajax": "fetchjobdag",
		"execid": execId,
		"jobid": jobId
	};

	var successHandler = function (data) {
		createModelFromAjaxCall(data, graphModel);
		graphModel.trigger("change:graph");
	};

	$.get(requestURL, request, successHandler, "json");
});
