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

	var request = {
		"project": projectName,
		"ajax": "fetchflowgraph",
		"flow": flowId
	};

	var successHandler = function (data) {
		createModelFromAjaxCall(data, graphModel);
		graphModel.trigger("change:graph");
	};

	$.get(requestUrl, request, successHandler, "json");
});
