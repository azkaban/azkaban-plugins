$(document).ready(function () {
	$("#run-button").click(function(event) {
		event.preventDefault();
		$.ajax({
			url: contextURL + "/reportal?ajax=run",
			type: "POST",
			data: $("form").serialize(),
			dataType: "json"
		}).done(function(data) {
			if(data.result == "success") {
				displaySuccess(data.message + " Redirecting in 5 seconds");
				setTimeout(function(){window.location.href = data.redirect;}, 5000);
			}
			else{
				displayError(data.error);
			}
		});
	});
});