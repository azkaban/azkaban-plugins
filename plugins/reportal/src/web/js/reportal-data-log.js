
var linkReplace = /(https?:\/\/(([-\w\.]+)+(:\d+)?(\/([\w/_\.]*(\?\S+)?)?)?))/g;
var newline = /(\r\n|\r|\n)/g;

var continueUpdate = function(){};

function beginUpdates() {
	var offset = 0;
	var log = "";
	var finished = false;
	var jobCompleted = false;
	var logSection = $("#logSection");
	var viewArea = $(".logViewer");
	var timeout = false;
	
	function requestUpdate() {
		$.ajax({
			url: contextURL + "/reportal",
			dataType: "json",
			data: {"id": projectId, "execId": execId, "jobId": jobId, "ajax": "log", "offset": offset, "length": 50000},
			error: function(data) {
				console.log(data);
				displayError("An error occurred while fetching logs.");
			},
			success: function(data) {
				if(data.result == "success") {
					readData(data);
				}
				else{
					displayError(data.error);
					finished = true;
					jobCompleted = true;
				}
				if(!finished || !jobCompleted) {
					var wait = 100;
					if(finished) {
						wait = 3000;
					}
					timeout = setTimeout(function() {
						requestUpdate();
					}, wait);
				}
			}
		});
	}

	function readData(data) {
		jobCompleted = data.completed;
		if(data.offset < offset || data.length == 0) {
			finished = true;
		}
		else {
			finished = false;
			offset = data.offset + data.length;
			log += data.log;
			var log2 = log.replace(linkReplace, "<a href=\"$1\" title=\"\">$1</a>").replace(newline, "<br>");
			logSection.html(log2);
			
			viewArea.animate({scrollTop: logSection.height()-viewArea.height()}, "slow");
		}
	}

	function resetRequest() {
		if(finished == true) {
			finished = false;
			jobCompleted = false;
			clearTimeout(timeout);
			requestUpdate();
		}
	}

	continueUpdate = resetRequest;

	requestUpdate();
	logSection.text("");
}

$(document).ready(function () {
	beginUpdates();
	$("#updateLogBtn").click(continueUpdate);
	$("#toggleLineWrap").click(function(){
		$("#logSection").toggleClass("no-wrap");
	});
});
