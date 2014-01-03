var extendedViewPanels = {};
var extendedDataModels = {};
var openJobDisplayCallback = function(nodeId, flowId, evt) {
	console.log("Open up data");
	
	var nodeInfoPanelID = flowId + ":" + nodeId + "-info";
	if ($("#" + nodeInfoPanelID).length) {
		$("#flowInfoBase").before(cloneStuff);
		extendedViewPanels[nodeInfoPanelID].showExtendedView(evt);
		return;
	}
	
	var cloneStuff = $("#flowInfoBase").clone();
	$(cloneStuff).attr("id", nodeInfoPanelID);
	
	$("#flowInfoBase").before(cloneStuff);
	var requestURL = contextURL + "/manager";
	var successHandler = function(data) {
		var graphModel = new azkaban.GraphModel();
		graphModel.set({
			id: data.id, 
			flow: data.flowData, 
			type: data.type, 
			props: data.props
		});

		var flowData = data.flowData;
		if (flowData) {
			createModelFromAjaxCall(flowData, graphModel);
		}
		
		var backboneView = new azkaban.FlowExtendedViewPanel({
			el: cloneStuff, 
			model: graphModel
		});
		extendedViewPanels[nodeInfoPanelID] = backboneView;
		extendedDataModels[nodeInfoPanelID] = graphModel;
		backboneView.showExtendedView(evt);
  };

  var request = {
		"project": projectName, 
		"ajax": "fetchflownodedata", 
		"flow": flowId, 
		"node": nodeId
	};

	$.get(requestURL, request, successHandler, "json");
}

var createModelFromAjaxCall = function(data, model) {
	var nodes = {};
	for (var i = 0; i < data.nodes.length; ++i) {
		var node = data.nodes[i];
		nodes[node.id] = node;
	}
	for (var i = 0; i < data.edges.length; ++i) {
		var edge = data.edges[i];
		var fromNode = nodes[edge.from];
		var toNode = nodes[edge.target];
		
		if (!fromNode.outNodes) {
			fromNode.outNodes = {};
		}
		fromNode.outNodes[toNode.id] = toNode;
		
		if (!toNode.inNodes) {
			toNode.inNodes = {};
		}
		toNode.inNodes[fromNode.id] = fromNode;
	}

	console.log("data fetched");
	model.set({data: data});
	model.set({nodes: nodes});
	model.set({disabled: {}});
};
