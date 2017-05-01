//Change to edit number of suggestions
var MAX_SUGGESTIONS = 4;

(function() {
  $('.results').hide()
  $('#uploadMsg').hide()

  // ON SEARCH
  function showResults(searchTerm, files, contexts, occurrences) {
    $('.results').append("<div class='results-header'>Results for \"" + searchTerm + "\":");

    console.log("hi");
    for(i = 0; i < files.length; i++) {
      var link = "../books/" + files[i];

      $('.results').append("<a class='results-link' href=" + link + ">" + files[i] + "</a");
      $('.results').append("<div class='results-text'>\"..." + contexts[i] + "...\"</div>");
      $('.results').append("<div class='results-text-occurrences'>Occurrences: " + occurrences[i] + "</div>");
    }

    $('.results').show();
    $('.matches').hide();
    $('.upload').hide();
  }


  $('input[type=text]').click(function() {
    $('input[type=file]').trigger('click');
  });

  $('input[type=file]').change(function() {
      $('input[type=text]').val($(this).val());
  });

  $('.matches').hide();

  // Get input and pass on to getJSON()
  $('.flexsearch-input').keyup(function(event) {
    var input = $(".flexsearch-input").val();
    $('.matches').html("");
    $('.matches').show();
    $('#uploadform').show();
    $('.results').hide();

    //short circuit if nothing in search box
    if(input.length == 0){
      return;
    }
    getJSON2(input);
  });

  $('#MR-submit').click(function() {
    var input = $(".flexsearch-input").val();

    var files = []
    var contexts = []
    var occurrences = []

    // !!!!!!!!!!!!!!!
    // THIS IS WHERE I GET THE JSON, CURRENTLY JUST GRABBING THE FILE FROM LOCAL after updating it with php
    $.post("../php/search.php",{search:input}).done(function(data2){
      console.log(data2);
      $.getJSON('../results/results.json', function(data) {
        for(i = 0; i < 100; i++) {
          if(data["results"][i] == null) break;
          obj = data["results"][i]
          files.push(obj["title"]);
          contexts.push(obj["context"]);
          occurrences.push(obj["occurances"]);
        }

        showResults(input, files, contexts, occurrences);
      });
    });
  });

  $('#spark-submit').click(function() {
    var input = $(".flexsearch-input").val();

    var files = []
    var contexts = []
    var occurrences = []

    // !!!!!!!!!!!!!!!
    // THIS IS WHERE I GET THE JSON, CURRENTLY JUST GRABBING THE FILE FROM LOCAL after updating it with php
    $.post("../php/spark-search.php",{search:input}).done(function(data2){
      console.log(data2);
      $.getJSON('../results/results.json', function(data) {
        for(i = 0; i < 100; i++) {
          if(data["results"][i] == null) break;
          obj = data["results"][i]
          files.push(obj["title"]);
          contexts.push(obj["context"]);
          occurrences.push(obj["occurances"]);
        }

        showResults(input, files, contexts, occurrences);
      });
    });
  });

  // Actual functionality
  function getJSON2(input) {
    $.ajax({
      //Hack to get around CORS complaint in chrome :(
      //Makes suggestions slow as hell but only not server side fix for xml in ajax
      url:"https://crossorigin.me/https://www.google.com/complete/search?output=toolbar&q="+input,
      type:"GET",
      dataType:"XML"
    })

    .done(function(xml) {
      var count = 0; //limits number of suggestions
      $(xml).find('suggestion').each(function() {
        search = $(this).attr('data');
        search = search.toLowerCase();

        if (input.length > 0 && count < MAX_SUGGESTIONS) {
          $('.matches').append("<a target=\"_blank\" href=\"_blank" + search + "\">" + search + "</a>");
          count++;
        }
      });
    })

    .fail(function() {
      console.log("Error");
    });
  }

  //file upload functionality
  $("#uploadform").submit(function(evt){
      evt.preventDefault();
      var formData = new FormData($(this)[0]);
   $.ajax({
       url: '../php/fileUpload.php',
       type: 'POST',
       data: formData,
       cache: false,
       contentType: false,
       enctype: 'multipart/form-data',
       processData: false,
       success: function (response) {
         console.log(response);
       }
   });
   $("#uploadform").hide();
   $('#uploadMsg').show()
   return false;
 });
})();
