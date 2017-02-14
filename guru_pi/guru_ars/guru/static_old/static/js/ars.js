
    $(document).ready(function(){

        $('#submitMail').click(function(){

            if($('#email').val() != '' && $('#subject').val() != ''){
                var title = $('#subject').val();
                var email = $('#email').val();
                var csrfmiddlewaretoken= $('input[name="csrfmiddlewaretoken"]').val()

                var data = '';
                var type = '';
                var chart = $('#results_graph').highcharts();

                if(chart){
                    var canv = document.createElement("canvas");
                    canv.style = "display: none";
                    canvg(canv, chart.getSVG());
                    var img = canv.toDataURL("image/png");
                    console.log('<img src"'+img+'">');
                    data = img;
                    type = "graph"
                }
                else{
                    data = '<html><body><table border="1">'+$("#result_table").html()+'</table></body></html>';
                    type = "table"
                }

                console.log(data);
                console.log(title);
                console.log(email);
                console.log(csrfmiddlewaretoken)
                $.ajax({
                    url: '/mail_gateway',
                    data: {'email': email, 'title': title, 'data': data, 'type': type, 'csrfmiddlewaretoken': csrfmiddlewaretoken },
                    type: 'POST',
                    dataType: 'json',
                    success: function(response){
                        console.log(response);
                        if(response.res == "successful"){
                            $('#msgDiv').html('<h5 style="color: green">Successfully Delivered</h5>');
                            setTimeout(function(){
                                $('#sendMailModal').modal('hide');
                            }, 2500);
                        }else{
                            $('#msgDiv').html('<h5 style="color: red">Error Delivering Mail</h5>');
                        }

                    }
                });
            }else{
                $('#msgDiv').html('<p style="color: red">Please Enter required Fields</p>')
            }
        });

        //Adding Class to body tag
        $("#submit").click(function(){
            $('body').addClass("show_result");
            submit_form();
        });

        //Displaying Debug Button
        $("#debugBtn").click(function(){
            //$('#result_raw_info').css("display","block");
            $("#result_raw_info").toggle();
        });


        //submit event to search
        $('#transcript').keypress(function (e) {
            if (e.which == 13) {
                $('body').addClass("show_result");
                submit_form();
                return false;
            }
        });


        //Display Modal
        $('#sendBtn').click(function(){
            $('#sendMailModal').modal('show');
        });


      //;(function(){

            var $SCRIPT_ROOT = "";
            var submit_form = function(e) {

            $('.loading').html('<img src="/static/loading.gif" />');

            $('.loading').show();

            $.getJSON($SCRIPT_ROOT+ '/search', {  

                query: $('input[name="query"]').val()

            }, function(data) {

                var result_final= '';

                console.log(data);

                var result_raw_info= JSON.stringify(data.info);

                if(data.result.type == "text"){

                    var table_header, table_body = '';

                    for(i=0; i< data.result.format.length; i++){

                        table_header += '<th>'+data.result.format[i]['field']+'</th>';
                    }
                    table_body+= '<table>'
                    for(i= 0; i< data.result.data.length; i++){
                        table_body+= '<tr>'
                        for(j= 0; j< data.result.format.length; j++){
                            table_body += '<td>'+data.result.data[i][j]+'</td>';
                            }
                        table_body+= '</tr>'
                    table_body+= '</table>'
                    }
                    //console.log(table_body);

                    $('#result_table thead tr').html(table_header);

                    $('#result_table tbody').html(table_body);

                    var temp_raw_info = "<h4 style = 'color: blue'>Debug Info: </h4><br>"+ result_raw_info;

                    $('#result_raw_info').html(temp_raw_info);

                    $('input[name="query"]').focus().select();

                    $('#debugBtn').css('display','block');
                    $('#sendBtn').css('display','block');
                    $('#finalResultShow').show();
                    $('#results_message').hide();
                    $('#results_graph').hide();
                    $("#result_table").show();
                    $('.loading').hide();

                }else if(data.result.type == "message"){
                    var temp_raw_info = "<h4 style = 'color: blue'>Debug Info: </h4><br>"+ result_raw_info;
                    $('#result_raw_info').html(temp_raw_info);
                    $('#debugBtn').css('display','block');
                    $('#sendBtn').css('display','none');
                    $('#results_message').html(data.result.data);
                    $('#finalResultShow').css({"height": "100px"});
                    $('#result_table').hide();
                    $('#results_graph').hide();
                    $('#finalResultShow').show();
                    $("#results_message").show();
                    $('.loading').hide();
                    console.log(data.result.data);

                }else if(data.result.type == "graph"){
                    var temp_raw_info = "<h4 style = 'color: blue'>Debug Info: </h4><br>"+ result_raw_info;
                    $('#result_raw_info').html(temp_raw_info);
                    $('#debugBtn').css('display','block');
                    $('#sendBtn').css('display','block');
                    $('#finalResultShow').css({"height": "450px"});
                    $('#result_table').hide();
                    $('#results_message').hide();
                    resultGraph(data);
                    $('#finalResultShow').show();
                    $("#results_graph").show();
                    $('.loading').hide();

                }
            })
            return false;
        };

      //}());


      $(function() {

      $('#result_raw_info').hide();

      $('.loading').hide();

      $('#finalResultShow').hide();

      var HR = window.HR = {};

      var currentQuery = "";

      var transcript = document.getElementById('transcript');

      function startDictation() {

        if (window.hasOwnProperty('webkitSpeechRecognition')) {

          var recognition = new webkitSpeechRecognition();

          recognition.continuous = false;
          recognition.interimResults = false;

          recognition.lang = "en-US";
          recognition.start();

          /*
          document.getElementById('result').text= '';
          document.getElementById('result_raw_info').text= '';
          document.getElementById('microphone').src="/static/assets/img/microphone-icon.png";
          */

          transcript.placeholder= "listening...";

          recognition.onresult = function(e) {
            currentQuery = transcript.value = e.results[0][0].transcript;

            console.log(transcript.value)
            //document.getElementById('microphone').src="/static/assets/img/microphone-icon-off.png";
            recognition.stop();

            transcript.placeholder= "Speak or Write";

            $('body').addClass("show_result");

            submit_form(transcript.value);

          };

          recognition.onerror = function(e) {
            recognition.stop();
            console.log(e);
            //document.getElementById('microphone').src="/static/assets/img/microphone-icon-off.png";
            document.getElementById('transcript').placeholder= "Speak or Write";
          }

        }
      }

      HR.startDictation = startDictation;
    });


    function resultGraph(data) {

      var format = data.result.format;

      data = data.result.data;

    console.log(data);

      var options = {
    "colors": [ "#5290e9", "#71b37c", "#ec932f", "#e14d57", "#965994", "#9d7952", "#cc527b", "#33867f", "#ada434" ],
    chart: {
            type: 'spline',
            backgroundColor: null,
        width: 928,
            height: 520
        },
        title: {
            text: 'Report'
        },
        subtitle: {
            text: ''
        },
        xAxis: {
            categories: data[0].date
        },
        yAxis: {
            title: {
                text: format[1].field
            }
        },
        tooltip: {
            crosshairs: true,
            shared: true
        },
        plotOptions: {
            spline: {
                marker: {
                    radius: 4,
                    lineColor: '#666666',
                    lineWidth: 1
                }
            }
        },
        series: []
      };

      var series;

      for (var index in data) {

    series = data[index];

        options.series.push({

      name: series.key,
          data: series.values
        });
      }

      $('#results_graph').highcharts(options);
    }


    });

