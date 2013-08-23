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