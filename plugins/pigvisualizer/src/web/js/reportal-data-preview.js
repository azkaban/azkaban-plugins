Array.prototype.transpose = function() {
	var a = this,
	w = a.length ? a.length : 0,
	h = a[0] instanceof Array ? a[0].length : 0;

	if(h === 0 || w === 0) { return []; }

	var i, j, t = [];

	for(i=0; i<h; i++) {
		t[i] = [];
		for(j=0; j<w; j++) {
			t[i][j] = a[j][i];
		}
	}
	return t;
};

Array.prototype.toInt = function() {
	var a = this;
	var t = [];

	for(i=0; i<a.length; i++) {
		t[i] = parseInt(a[i]);
		if(t[i] == NaN) {
			t[i] = 0;
		}
	}
	return t;
};

Array.prototype.isAllInt = function() {
	for(i=0; i<this.length; i++) {
		if(isNaN(parseInt(this[i]))) {
			return false;
		}
	}
	return true;
};

var data = original.transpose();

var doNothing = function(){};

var reportalChart = $("#report-chart");
var buttonList = $("#buttons");
var graphButtons = $(".graph");
var barGraphButtons = $(".bar-graph");
var columnSelect = $(".column-data");
var labelSelect = $(".column-title");
var saveGraph = $(".save-graph");
barGraphButtons.hide();
columnSelect.hide();
labelSelect.hide();
saveGraph.hide();

var columnSelectAction = doNothing;
var labelSelectAction = doNothing;
var plotLastGraph = doNothing;

columnSelect.click(function() {
	columnSelectAction($(this));
});

labelSelect.click(function() {
	labelSelectAction($(this));
});

saveGraph.click(function() {
	$.ajax({
		url: contextURL + "/reportal?ajax=graph&id=" + projectId + "&hash=" + encodeURIComponent(location.hash),
		dataType: "json"
	}).done(function(data) {
		if(data.result == "success") {
			displaySuccess(data.message);
		}
		else{
			displayError(data.error);
		}
	});
});

function plotDefault() {
	var defaultPlot = parseInt(getHashValue("default-plot"));
	var barType = parseInt(getHashValue("type")) || 0;
	var labelIndex = parseInt(getHashValue("label")) || 0;
	var data1Index = parseInt(getHashValue("data1")) || 0;
	var data2Index = getHashValue("data2") || "0";
	data2Index = data2Index.split(",");
	var label = data[labelIndex];
	var data1 = data[data1Index];
	var data2 = [];
	var data2Indecies = [];
	for (var i = 0; i < data2Index.length; i++) {
		var index = parseInt(data2Index[i]);
		data2Indecies.push(index);
		data2.push(data[index].toInt());
	};
	switch(defaultPlot)
	{
		case 1:
			graphLinesStart(data1, data2, data1Index, data2Indecies);
			break;
		case 2:
			graphBarsStart(barType, label, data2, labelIndex, data2Indecies);
			break;
		case 3:
			graphPieStart(label, data1, labelIndex, data1Index);
			break;
		default:
			break;
	}
}

$(document).ready(function () {
	plotDefault();
});

function columnSelectShow(text, callback, keepVisibility) {
	text = text || "Select Data";
	callback = callback || doNothing;
	columnSelectAction = callback;
	columnSelect.html(text);
	if(!keepVisibility) {
		var availableCols = data.length;
		columnSelect.show();
		for (var i = 0; i < data.length; i++) {
			if(!data[i].isAllInt()) {
				columnSelect.eq(i).hide();
				availableCols--;
			}
		}
		return availableCols;
	}
}

//Line graphs
buttonList.delegate(".graph-lines", "click", function(){
	graphButtons.hide();
	graphLinesStart();
	saveGraph.hide();
});

function graphLinesStart(x, y, xIndexInput, yIndexInput) {
	var columnX = 0;
	var columnYArray = [];

	var xIndex = 0;
	var yIndex = [];

	var graphLineSelectX = function(item) {
		var index = getElementIndex(item);
		xIndex = index.toString();
		columnX = data[index];
		columnSelectShow("Select Y" + (columnYArray.length + 1), graphLineSelectY, true);
		item.hide();
	};

	var graphLineSelectY = function(item) {
		var index = getElementIndex(item);
		yIndex.push(index.toString());
		columnYArray.push(data[index]);
		columnSelectShow("Select Y" + (columnYArray.length + 1), graphLineSelectY, true);
		item.hide();
		graphSelectEnd();
	};

	var graphSelectEnd = function() {
		setHashString(getHashString(1, 0, 0, xIndex, yIndex));
		plotLastGraph = plotLastGraphLine;
		plotLastGraph();
		graphButtons.show();
		saveGraph.show();
	}

	var plotLastGraphLine = function() {
		plotGraphLines(columnX, columnYArray);
	}


	reportalChart.html("");
	if(x && y) {
		columnX = x;
		columnYArray = y;
		xIndex = xIndexInput.toString();
		yIndex = yIndexInput;
		graphSelectEnd();
	}
	else{
		var columns = columnSelectShow("Select X", graphLineSelectX);
		if(columns < 2) {
			displayError("There is not enough columns with numeric data to graph.");
			graphButtons.show();
			columnSelectAction = doNothing;
			labelSelectAction = doNothing;
			columnSelect.hide();
			labelSelect.hide();
		}
	}
}


//Bar graphs
buttonList.delegate(".graph-bar", "click", function(){
	graphButtons.hide();
	columnSelect.hide();
	barGraphButtons.show();
	saveGraph.hide();
});

buttonList.delegate(".vertical-series", "click", function(){
	graphBarsStart(0);
});

buttonList.delegate(".vertical-stacked", "click", function(){
	graphBarsStart(1);
});

buttonList.delegate(".horizontal-series", "click", function(){
	graphBarsStart(2);
});

buttonList.delegate(".horizontal-stacked", "click", function(){
	graphBarsStart(3);
});

function graphBarsStart(type, label, dataArray, labelIndexInput, dataIndexInput) {
	var barsArray = [];
	var barsLabel = [];

	var labelIndex = 0;
	var dataIndex = [];

	var graphSelect = function(item) {
		var index = getElementIndex(item);
		dataIndex.push(index);
		barsArray.push(data[index].toInt());
		item.hide();
		graphSelectEnd();
	};

	var graphSelectLabel = function(item) {
		var index = getElementIndex(item);
		labelIndex = index.toString();
		barsLabel = data[index];
		labelSelect.hide();
		graphSelectEnd();
	};

	var graphSelectEnd = function() {
		setHashString(getHashString(2, type, labelIndex, false, dataIndex));
		plotLastGraph = plotLastGraphBar;
		plotLastGraph();
		graphButtons.show();
		saveGraph.show();
	}

	var plotLastGraphBar = function() {
		plotGraphBars(type, barsArray, barsLabel);
	}

	reportalChart.html("");
	if(label && dataArray) {
		barsLabel = label;
		barsArray = dataArray;
		labelIndex = labelIndexInput.toString();
		dataIndex = dataIndexInput;
		graphSelectEnd();
	}
	else {
		graphButtons.hide();
		barGraphButtons.hide();
		labelSelectAction = graphSelectLabel;
		labelSelect.show();
		var columns = columnSelectShow(false, graphSelect);
		if(columns < 1) {
			displayError("There is not enough columns with numeric data to graph.");
			graphButtons.show();
			columnSelectAction = doNothing;
			labelSelectAction = doNothing;
			columnSelect.hide();
			labelSelect.hide();
		}
	}
}


//Pie graphs
buttonList.delegate(".graph-pie", "click", function(){
	graphButtons.hide();
	graphPieStart();
	saveGraph.hide();
});

function graphPieStart(label, dataArray, labelIndexInput, dataIndexInput) {
	var pieData = [];
	var pieLabel = [];

	var labelIndex = 0;
	var dataIndex = 0;

	var graphSelect = function(item) {
		var index = getElementIndex(item);
		dataIndex = index.toString();
		pieData = data[index];
		columnSelect.hide();
		graphSelectEnd();
	};

	var graphSelectLabel = function(item) {
		var index = getElementIndex(item);
		labelIndex = index.toString();
		pieLabel = data[index];
		labelSelect.hide();
		graphSelectEnd();
	};

	var graphSelectEnd = function() {
		setHashString(getHashString(3, 0, labelIndex, dataIndex));
		plotLastGraph = plotLastGraphPie;
		plotLastGraph();
		graphButtons.show();
		saveGraph.show();
	}

	var plotLastGraphPie = function() {
		plotGraphPie(pieData, pieLabel);
	}

	reportalChart.html("");
	if(label && dataArray) {
		pieLabel = label;
		pieData = dataArray;
		labelIndex = labelIndexInput.toString();
		dataIndex = dataIndexInput.toString();
		graphSelectEnd();
	}
	else {
		labelSelectAction = graphSelectLabel;
		labelSelect.show();
		var columns = columnSelectShow(false, graphSelect);
		if(columns < 1) {
			displayError("There is not enough columns with numeric data to graph.");
			graphButtons.show();
			columnSelectAction = doNothing;
			labelSelectAction = doNothing;
			columnSelect.hide();
			labelSelect.hide();
		}
	}
}

//Resize update
var graphContainer = $("#report-chart-container");
var graphContainerWidth = graphContainer.width();
var graphContainerHeight = graphContainer.height();
graphContainer.mouseup(function(){
	if(graphContainerWidth != graphContainer.width() || graphContainerHeight != graphContainer.height()) {
		graphContainerWidth = graphContainer.width();
		graphContainerHeight = graphContainer.height();
		plotLastGraph();
	}
});

function plotGraphLines(x, yArray) {
	var r = prepareRaphael();

	r.linechart(20, 10, graphContainerWidth - 30, graphContainerHeight - 20, x, yArray, { shade: true, nostroke: false, axis: "0 0 1 1", symbol: "circle", smooth: true })
		.hoverColumn(function () {
		this.tags = r.set();
		for (var i = 0, ii = this.y.length; i < ii; i++) {
			this.tags.push(r.tag(this.x, this.y[i], this.axis + ", " + this.values[i], 160, 10).insertBefore(this).attr([{ fill: "#fff" }, { fill: this.symbols[i].attr("fill") }]));
		}
	}, function () {
		this.tags && this.tags.remove();
	});
}

function plotGraphBars(type, barsArray, label) {
	var r = prepareRaphael();

	var fin = function () {
		this.flag = r.popup(this.bar.x, this.bar.y, this.bar.value || "0").insertBefore(this);
	};
	var fout = function () {
		this.flag.animate({opacity: 0}, 300, function () {this.remove();});
	};
	var fin2 = function () {
		var y = [], res = [];
		for (var i = this.bars.length; i--;) {
			y.push(this.bars[i].y);
			res.push(this.bars[i].value || "0");
		}
		this.flag = r.popup(this.bars[0].x, Math.min.apply(Math, y), res.join(", ")).insertBefore(this);
	};
	var fout2 = function () {
		this.flag.animate({opacity: 0}, 300, function () {this.remove();});
	};

	var barChart;

	console.log(barsArray);

	if(type == 0) {
		barChart = r.barchart(30, 10, graphContainerWidth - 40, graphContainerHeight - 20, barsArray).hover(fin, fout);
	}
	else if(type == 1) {
		barChart = r.barchart(30, 10, graphContainerWidth - 40, graphContainerHeight - 20, barsArray, {stacked: true}).hoverColumn(fin2, fout2);
	}
	else if(type == 2) {
		barChart = r.hbarchart(30, 10, graphContainerWidth - 40, graphContainerHeight - 20, barsArray).hover(fin, fout);
	}
	else if(type == 3) {
		barChart = r.hbarchart(30, 10, graphContainerWidth - 40, graphContainerHeight - 20, barsArray, {stacked: true}).hover(fin, fout);
	}

	function customLabel(labels, horizontal) {
		labels = labels || [];
		this.labels = r.set();
		var i = 0;
		for (var j = 0; j < this.bars[0].length; j++) {
			var x = 0, y = 0;
			if(horizontal) {
				for (i = 0; i < this.bars.length; i++) {
					y += this.bars[i][j].y;
				}
				x = this.bars[0][j].x - this.bars[0][j].w - 5;
				y /= this.bars.length;
				r.text(x, y, labels[j] || "").attr({"text-anchor": "end", "font": "14px sans-serif"});
			}
			else {
				for (i = 0; i < this.bars.length; i++) {
					x += this.bars[i][j].x;
				}
				x /= this.bars.length;
				y = this.bars[0][j].y + this.bars[0][j].h + 20;
				r.text(x, y, labels[j] || "").attr({"font": "14px sans-serif"});
			}
		}
		return this;
	}
	barChart.customLabel = customLabel;
	barChart.customLabel(label, type==2||type==3);
}

function plotGraphPie(pieArray, pieTitle) {
	var r = prepareRaphael();
	var pie = r.piechart(graphContainerWidth/2, (graphContainerHeight)/2, Math.min(graphContainerWidth, graphContainerHeight)/2/1.1, pieArray.toInt(), {legend: pieTitle});

	pie.hover(function () {
		this.sector.stop();
		this.sector.scale(1.1, 1.1, this.cx, this.cy);

		if (this.label) {
			this.label[0].stop();
			this.label[0].attr({ r: 7.5 });
			this.label[1].attr({ "font-weight": 800 });
		}
	}, function () {
		this.sector.animate({ transform: 's1 1 ' + this.cx + ' ' + this.cy }, 500, "bounce");

		if (this.label) {
			this.label[0].animate({ r: 5 }, 500, "bounce");
			this.label[1].attr({ "font-weight": 400 });
		}
	});
}

function prepareRaphael() {
	$("#report-chart-container").show();
	reportalChart.html("");
	return Raphael("report-chart");
}

function getElementIndex(item) {
	var element = $(item).closest("th");
	return element.parent().children().index(element);
}

function sortByIndex(arry, index) {
	arry.sort(function(a, b){return a[index]-b[index]});
	return arry;
}

function getHashValue(key) {
	var match = location.hash.match(new RegExp(key+'=([^&]*)'));
	if(match) {
		return match[1];
	}
}

function getHashString(defaultPlot, type, label, data1, data2) {
	var string = "#";
	if(defaultPlot) {
		string += "default-plot=" + defaultPlot + "&";
	}
	if(type) {
		string += "type=" + type + "&";
	}
	if(label) {
		string += "label=" + label + "&";
	}
	if(data1) {
		string += "data1=" + data1 + "&";
	}
	if(data2) {
		string += "data2=";
		for (var i = 0; i < data2.length; i++) {
			if(i > 0) {
				string += ",";
			}
			string += data2[i];
		};
	}
	return string;
}

function setHashString(string) {
	location.hash = string;
}
