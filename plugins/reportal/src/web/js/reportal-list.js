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
  var reportalTable = $("#reportalTable");
  var tableBody = $("#reportalTable").find("tbody").eq(0);
  reportalTable.tablesorter({sortList:[[0,0]]});
  var template = $("#action-template").html();
  var inputBookmark = $("#facet_bookmarked")[0];
  var inputSubscribe= $("#facet_subscribed")[0];
  var inputDate = $("#facet_date_created")[0];
  var inputDateStart = $("#date_created_from");
  var inputDateEnd = $("#date_created_to");
  var inputOwner = $("#facet_owner")[0];
  var inputOwnerText = $("#owner");

  function displayData() {
    //Clear the table
    tableBody.html("");
    //Filter data
    var displayReportals = filterData();

    if(displayReportals.length == 0) {
      tableBody.append("<tr><td class='last' colspan='3'>No reportal found</td></tr>");
    }
    else {
      for (var i = 0; i < displayReportals.length; i++) {
        var shown = displayReportals[i].shown;
        if(!shown) {
          continue;
        }
        var id = displayReportals[i].id;
        var title = displayReportals[i].title;
        var time = displayReportals[i].time;
        var timeText = displayReportals[i].timeText;
        var user = displayReportals[i].user;
        var scheduled = displayReportals[i].scheduled;
        var scheduledRepeating = displayReportals[i].scheduledRepeating;
        var bookmark = displayReportals[i].bookmark;
        var subscribe = displayReportals[i].subscribe;

        if(scheduled) {
          if(scheduledRepeating) {
            title = title + " <span class=\"label\">Scheduled Repeating</span>";
          }
          else {
            title = title + " <span class=\"label\">Scheduled</span>";
          }
        }

        var row = $("<tr></tr>");
        row.append("<td><a href='" + contextURL + "/reportal?view&id=" + id + "'>" + title + "</a>" + template + "</td>");
        row.append("<td>" + timeText + "</td>");
        row.append("<td>" + user + "</td>");
        row[0].item = displayReportals[i];

        row.find(".button-edit").eq(0).attr("href", contextURL + "/reportal?edit&id=" + id);
        row.find(".button-view").eq(0).attr("href", contextURL + "/reportal?view&id=" + id);
        row.find(".button-run").eq(0).attr("href", contextURL + "/reportal?run&id=" + id);

        row.find(".button-delete").eq(0).click(function(event) {
          event.preventDefault();
          if(confirm("Deletion confirmation", "Are you sure that you want to delete this reportal?")) {
            var item = $(this).closest('tr')[0].item;
            $.ajax({
              url: contextURL + "/reportal?ajax=delete&id=" + item.id,
              dataType: "json"
            }).done(function(data) {
              if(data.result == "success") {
                //Remove that line of item
                reportals.splice(reportals.indexOf(item), 1);
                displayData();
              }
              else{
                displayError(data.error);
              }
            });
          }
        });

        var bookmarkItem = row.find(".button-bookmark").eq(0).click(function(event) {
          event.preventDefault();
          var theRow = $(this).closest('tr').eq(0);
          var item = theRow[0].item;
          $.ajax({
            url: contextURL + "/reportal?ajax=bookmark&id=" + item.id,
            dataType: "json"
          }).done(function(data) {
            if(data.result == "success") {
              item.bookmark = data.bookmark;
              iconColor(theRow.find(".button-bookmark").eq(0).find("span"), "icon-bookmark", item.bookmark);
            }
            else{
              displayError(data.error);
            }
          });
        });
        iconColor(bookmarkItem.find("span"), "icon-bookmark", bookmark);

        var subscribeItem = row.find(".button-subscribe").eq(0).click(function(event) {
          event.preventDefault();
          var theRow = $(this).closest('tr').eq(0);
          var item = theRow[0].item;
          var email = "";
          $.ajax({
            url: contextURL + "/reportal?ajax=subscribe&id=" + item.id,
            dataType: "json"
          }).done(function(data) {
            if(data.result == "success") {
              item.subscribe = data.subscribe;
              iconColor(theRow.find(".button-subscribe").eq(0).find("span"), "icon-mail", item.subscribe);
            }
            else{
              displayError(data.error);
            }
          });
        });
        iconColor(subscribeItem.find("span"), "icon-mail", subscribe);

        tableBody.append(row);
      };
    }
    reportalTable.trigger("update");
  }

  function filterData() {
    var reportalsCopy = reportals.slice(0);
    for (var i = reportalsCopy.length - 1; i >= 0; i--) {
      reportalsCopy[i].shown = true;
      //Filter it out
      if(inputBookmark.checked && !reportalsCopy[i].bookmark) {
        reportalsCopy[i].shown = false;
      }
      else if(inputSubscribe.checked && !reportalsCopy[i].subscribe) {
        reportalsCopy[i].shown = false;
      }
      else if(inputDate.checked) {
        var start = Date.parse(inputDateStart.val());
        var end = Date.parse(inputDateEnd.val());
        if((start != NaN && reportalsCopy[i].time < start) || (end != NaN && reportalsCopy[i].time > end)) {
          reportalsCopy[i].shown = false;
        }
      }
      else if(inputOwner.checked && reportalsCopy[i].user.toLowerCase().indexOf(inputOwnerText.val().toLowerCase()) != 0) {
        reportalsCopy[i].shown = false;
      }
    };
    return reportals;
  }

  function iconColor(element, className, dark) {
    element.removeClass(className).removeClass(className + "-dark");
    if(dark) {
      element.addClass(className + "-dark");
    }
    else {
      element.addClass(className);
    }
  }

  $("#search-facet-form").delegate("input", "change", function (event) {
    displayData();
  }).delegate("input", "input", function (event) {
    displayData();
  });

  $("#reportalTable").delegate(".btn-dropdown", "click", function (event) {
    event.stopPropagation();
    // Close any other menu that was open.
    $('.btn-dropdown.visible').not(this).toggleClass('visible').children('.dropdown-menu').toggle();
    $(this).children('.dropdown-menu').toggle();
    $(this).toggleClass('visible')
  });

  $("html").click(function(){
    $('.btn-dropdown.visible').toggleClass('visible').children('.dropdown-menu').toggle();
  });

  displayData();
});
