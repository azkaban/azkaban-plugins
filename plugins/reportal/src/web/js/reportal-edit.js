$(document).ready(function () {

	var queryListObject = $("#query-list");
	var queryNumberObject = $("#queryNumber");
	var queryTemplate = $("#query-template").find("li").eq(0);
	var variableListObject = $("#variable-list");
	var variableNumberObject = $("#variableNumber");
	var variableTemplate = $("#variable-template").find("li").eq(0);
	var scheduleFields = $("#schedule-fields");
	var variableFields = $("#variable-fields");

	function updateQueryListOrder(){
		var elements = queryListObject.find("li");
		var first = 0;
		var last = elements.length - 1;

		$.each(elements, function(index){
			var element = $(this);
			element.attr("id", "query" + index);
			$.each(element.find("input").add(element.find("select")).add(element.find("textarea")), function(i){setNameFromTemplate($(this), index);} );
			var btnBumpUp = element.find('.bump-up');
			var btnBumpDown = element.find('.bump-down');
			var btnDelete = element.find('.delete');

			$(btnBumpUp).removeClass('first last disabled');
			$(btnBumpDown).removeClass('first last disabled');
			element.removeClass('first last')

			if (index == first) {
				element.addClass('first');
				$(btnBumpUp).addClass('disabled');
			}
			if (index == last) {
				element.addClass('last');
				$(btnBumpDown).addClass('disabled');
			}
			if(last == first) {
				$(btnDelete).hide();
			}
			else {
				$(btnDelete).show();
			}

			var textArea = element.find("textarea")[0];
			if(!textArea.codeMirror) {
				var selectType = element.find("select").val();
				var mime = "text/x-sql";
				if(selectType.indexOf("Pig") != -1) {
					mime = "text/x-pig";
				}
				textArea.codeMirror = CodeMirror.fromTextArea(textArea, {lineNumbers: false, theme: "solarized dark", mode: mime});
			}
		});

		queryNumberObject.attr("value", elements.length);
	}

	function updateVariableListOrder(){
		var elements = variableListObject.find("li");
		var first = 0;
		var last = elements.length - 1;

		$.each(elements, function(index){
			var element = $(this);
			element.attr("id", "query" + index);
			$.each(element.find("input"), function(i){setNameFromTemplate($(this), index);} );
		});

		variableNumberObject.attr("value", elements.length);
	}

	function setNameFromTemplate(element, index){
		if(typeof element.attr("nametemplate") != 'undefined') {
			element.attr("name", element.attr("nametemplate").replace(/\#/g, index));
		}
	}

	function schedulOptionChangeHandler(item) {
		if(item.checked) {
			variableFields.hide();
			scheduleFields.show();
		} else {
			variableFields.show();
			scheduleFields.hide();
		}
	};

	queryListObject.delegate(".delete", "click", function (event) {
		event.preventDefault();
		if (confirm("Are you sure you want to delete this?")) {
			$(this).closest('li').remove();
			updateQueryListOrder();
		} 
	})
	.delegate('.bump-up', 'click', function (event) {
		event.preventDefault();
		var element = $(this).closest('li');
		var siblings = element.siblings();
		var index = element.index();
		var newIndex = index - 1;

		if (newIndex < 0) {
			return;
		}

		$(element).insertBefore(siblings[newIndex]).hide().fadeIn();
		updateQueryListOrder();
	})
	.delegate('.bump-down', 'click', function (event) {
		event.preventDefault();
		var element = $(this).closest('li');
		var siblings = element.siblings();
		var index = element.index();
		var newIndex = index - 1;

		if (index >= siblings.length) {
			return;
		}

		$(element).insertAfter(siblings[index]).hide().fadeIn();
		updateQueryListOrder();
	})
	.delegate("select", "change", function (event) {
		var element = $(this).closest('li');
		var textArea = element.find("textarea")[0];
		if(textArea.codeMirror) {
			var selectType = element.find("select").val();
			var mime = "text/x-sql";
			if(selectType.indexOf("Pig") != -1) {
				mime = "text/x-pig";
			}

			textArea.codeMirror.setOption("mode", mime);
		}
	});

	variableListObject.delegate(".delete", "click", function (event) {
		event.preventDefault();
		if (confirm("Are you sure you want to delete this?")) {
			$(this).closest('li').remove();
			updateVariableListOrder();
		} 
	});

	$("#buttonAddQuery").click(function(){
		queryTemplate.clone().appendTo(queryListObject);
		updateQueryListOrder();
	});

	$("#buttonAddVariable").click(function(){
		variableTemplate.clone().appendTo(variableListObject);
		updateVariableListOrder();
	});

	$("#schedule-options").change(function (event) {
		schedulOptionChangeHandler(this);
	});

	//Load schedule options
	schedulOptionChangeHandler($("#schedule-options")[0]);

	//Load starting queries
	for (var i = 0; i < startQueries.length; i++) {
		startQueries[i].script = $("#script" + startQueries[i].num).html();
	};

	function addInitialQueries() {
		function addQuery(item) {
			var element = queryTemplate.clone();
			element.find(".querytitle").eq(0).val(item.title);
			var select = element.find(".querytype").eq(0).val(item.type);
			element.find(".queryscript").eq(0).val(item.script);
			element.appendTo(queryListObject);
		}
		for (var i = 0; i < startQueries.length; i++) {
			addQuery(startQueries[i]);
		};
		updateQueryListOrder();
	}
	addInitialQueries();

	function addInitialVariables() {
		function addVariable(item) {
			var element = variableTemplate.clone();
			element.find(".variabletitle").eq(0).val(item.title);
			element.find(".variablename").eq(0).val(item.name);
			element.appendTo(variableListObject);
		}
		for (var i = 0; i < startVariables.length; i++) {
			addVariable(startVariables[i]);
		};
		updateVariableListOrder();
	}
	addInitialVariables();
});