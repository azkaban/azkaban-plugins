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

$(document).ready(function () {

	var queryListObject = $("#query-list");
	queryListObject.numberObject = $("#queryNumber");
	queryListObject.template = $("#query-template").find("li").eq(0);
	queryListObject.type = "query";

	var variableListObject = $("#variable-list");
	variableListObject.numberObject = $("#variableNumber");
	variableListObject.template = $("#variable-template").find("li").eq(0);
	variableListObject.type = "variable";

	var scheduleOptions = $("#schedule-options");
	var scheduleFields = $("#schedule-fields");
	var scheduleDate = $("#schedule-date");
	var scheduleRepeat = $("#schedule-repeat");
	var scheduleRepeatFields = $("#schedule-repeat-fields");
	
	var buttonAddQuery = $("#buttonAddQuery");
	var buttonAddVariable = $("#buttonAddVariable");

	function updateListOrder(listObject, requireAtLeastOneElement){
		var elements = listObject.find("li");
		var first = 0;
		var last = elements.length - 1;

		$.each(elements, function(index){
			var element = $(this);
			element.attr("id", listObject.type + index);
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
			if(last == first && requireAtLeastOneElement) {
				$(btnDelete).hide();
			}
			else {
				$(btnDelete).show();
			}

			var textArea = element.find("textarea")[0];
			if(textArea && !textArea.codeMirror) {
				var selectType = element.find("select").val();
				var mime = "text/x-sql";
				if(selectType.indexOf("Pig") != -1) {
					mime = "text/x-pig";
				}
				textArea.codeMirror = CodeMirror.fromTextArea(textArea, {
					lineNumbers: true,
					lineWrapping: true,
					theme: "solarized dark",
					mode: mime
				});

				// Enable resizing of the CodeMirror code editor.
				$('.CodeMirror').resizable({
			        resize: function() {
			            textArea.codeMirror.setSize($(this).width(), $(this).height());
			        }
			    });
			}
		});

		listObject.numberObject.attr("value", elements.length);
	}

	function setNameFromTemplate(element, index){
		if(typeof element.attr("nametemplate") != 'undefined') {
			element.attr("name", element.attr("nametemplate").replace(/\#/g, index));
		}
	}

	function scheduleOptionChangeHandler(item) {
		if(item && item.checked) {
			scheduleFields.show();
		} else {
			scheduleFields.hide();
		}
	};

	function scheduleRepeatOptionChangeHandler(item) {
		if (item && item.checked) {
			scheduleRepeatFields.show();
		} else {
			scheduleRepeatFields.hide();
		}
	}

	function handleDelete(event, listObject, requireAtLeastOneElement) {
		event.preventDefault();
		if (confirm("Are you sure you want to delete this?")) {
			$(this).closest('li').remove();
			updateListOrder(listObject, requireAtLeastOneElement);
		} 
	}

	function handleBumpUp(event, listObject) {
		event.preventDefault();
		var element = $(this).closest('li');
		var siblings = element.siblings();
		var index = element.index();
		var newIndex = index - 1;

		if (newIndex < 0) {
			return;
		}

		$(element).insertBefore(siblings[newIndex]).hide().fadeIn();
		updateListOrder(listObject);
	}

	function handleBumpDown(event, listObject) {
		event.preventDefault();
		var element = $(this).closest('li');
		var siblings = element.siblings();
		var index = element.index();
		var newIndex = index - 1;

		if (index >= siblings.length) {
			return;
		}

		$(element).insertAfter(siblings[index]).hide().fadeIn();
		updateListOrder(listObject);
	}

	queryListObject.delegate(".delete", "click", function (event) {
		handleDelete.call(this, event, queryListObject, true);
	})
	.delegate('.bump-up', 'click', function (event) {
		handleBumpUp.call(this, event, queryListObject);
	})
	.delegate('.bump-down', 'click', function (event) {
		handleBumpDown.call(this, event, queryListObject);
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
		handleDelete.call(this, event, variableListObject);
	})
	.delegate('.bump-up', 'click', function (event) {
		handleBumpUp.call(this, event, variableListObject);
	})
	.delegate('.bump-down', 'click', function (event) {
		handleBumpDown.call(this, event, variableListObject);
	});

	function handleAdd(listObject) {
		listObject.template.clone().appendTo(listObject);
		updateListOrder(listObject);
	}

	buttonAddQuery.click(function(){
		handleAdd(queryListObject);
	});

	buttonAddVariable.click(function(){
		handleAdd(variableListObject);
	});

	scheduleOptions.change(function (event) {
		scheduleOptionChangeHandler(this);
	});

	scheduleRepeat.change(function(){
		scheduleRepeatOptionChangeHandler(this);
	})

	//Load schedule options
	scheduleOptionChangeHandler(scheduleOptions[0]);
	scheduleRepeatOptionChangeHandler(scheduleRepeat[0])

	function addInitialQueries() {
		function addQuery(item) {
			var element = queryListObject.template.clone();
			element.find(".querytitle").eq(0).val(item.title);
			var select = element.find(".querytype").eq(0).val(item.type);
			element.find(".queryscript").eq(0).val(item.script);
			element.appendTo(queryListObject);
		}
		for (var i = 0; i < startQueries.length; i++) {
			addQuery(startQueries[i]);
		};
		updateListOrder(queryListObject, true);
	}
	addInitialQueries();

	function addInitialVariables() {
		function addVariable(item) {
			var element = variableListObject.template.clone();
			element.find(".variabletitle").eq(0).val(item.title);
			element.find(".variablename").eq(0).val(item.name);
			element.appendTo(variableListObject);
		}
		for (var i = 0; i < startVariables.length; i++) {
			addVariable(startVariables[i]);
		};
		updateListOrder(variableListObject);
	}
	addInitialVariables();
	
	scheduleDate.datetimepicker({pickTime: false});
});