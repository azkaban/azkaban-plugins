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
