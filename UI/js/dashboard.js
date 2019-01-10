$(document).ready(function () {

    var updateInterval = 3000
    var realtime = 'on'

    $('#dashboard').click(function () {
        query_api()
    });

    function query_api() {

        var apiURL_query_platform = "http://192.168.0.102:5000/api/platforms"
        $.ajax({
            url: apiURL_query_platform, success: function (platforms) {
                console.log(platforms)
                $('#main_content').empty()
                realtime = 'on'
                for (var i = 0; i < platforms.length; i++) {
                    $('#main_content').append(render_platform(platforms[i])).hide().fadeIn('slow')
                }
            }
        });

    };


    function render_platform(platform) {
        var arr = []
        arr = ['<div class="box box-default" id=\"', platform['PlatformId'], '\">',


            '<a href="#dashboard_' + platform['PlatformId'] + '\" style="color: #333;" role="button" data-toggle="collapse" aria-expanded="false" aria-controls="dashboard_' + platform['PlatformId'] + '\">',
            '<div class="box-header with-border info-box info_platform">',
            '<span class="info-box-icon"><img src="', 'images/', platform['PlatformType'], '_logo.png', '" width="90" height="90"></span>',
            '<div class="info-box-content">',
            '<span class="info-box-header">', platform['PlatformName'], '</span>',
            '<span class="info-box-text">Type: ', platform['PlatformType'], '</span>',
            '<span class="info-box-text">End Point: ', platform['PlatformHost'] + ":" + platform['PlatformPort'], '</span>',
            '</div><!-- /.info-box-content -->',
            '</div>',
            '<!-- /.box-header -->',
            '</a>',

            '<div class="box-body collapse" id =\"dashboard_' + platform['PlatformId'] + '\">',
            '</div>',

            '</div>'

        ]


        var apiURL_query_source = "http://192.168.0.102:5000/api/sources/platform_id/" + platform['PlatformId']
        arr_sources = []
        $.ajax({
            url: apiURL_query_source, success: function (sources) {
                console.log(sources)
                // $('#main_content').empty()
                for (var i = 0; i < sources.length; i++) {
                    $("#dashboard_" + platform['PlatformId']).append(render_source(sources[i]));
                    // $('#main_content').append(render_source(sources[i])).hide().fadeIn('slow')
                }
            }
        });

        return arr.join('')
        // <div class="box box-warning"><div class="box-header with-border"><h3 class="box-title">Collapsable</h3><div class="box-tools pull-right"><button type="button" class="btn btn-box-tool" data-widget="collapse"><i class="fa fa-minus"></i></button></div><!-- /.box-tools --></div><!-- /.box-header --><div class="box-body" style="">The body of the box</div><!-- /.box-body --></div>
    };

    function render_source(source) {
        var arr = []

        if (source["metrics"].length == 1) {
            arr = [
                '<div class="info_source" id=\"' + source['information']['SourceId'] + '\">',

                render_metric(source['metrics'][0]),
                '</div>',
            ]
        } else if (source["metrics"].length > 1) {
            if (source['information']['SourceType'] == 'Thing') {

                var metric = []
                for (var i = 0; i < source['metrics'].length; i++) {
                    metric.push(render_metric(source['metrics'][i]));
                }

                arr = [
                    '<div class="info_source" id=\"' + source['information']['SourceId'] + '\"><strong>' + source["information"]['ThingName'] + '</strong>',
                    metric.join(''),
                    '</div>',
                ]

            }


        }

        return arr.join('')
    }


    function render_metric(metric) {
        var arr = []

        var render_value=""

        if (metric["MetricDomain"] == 'switch'){
            if(metric['DataPoint']['Value'] == 'on'){
                render_value = '<span class="pull-right"><label class="switch"><input type="checkbox" checked><span class="slider round"></span></label></span>'
            }else{
                render_value = '<span class="pull-right"><label class="switch"><input type="checkbox"><span class="slider round"></span></label></span>'
            }
        }else{
            render_value = '<span class="pull-right">' + metric["DataPoint"]["Value"] + '</span>'
        }

        arr = [
            '<div class="info_metric" id=\"' + metric['MetricId'] +'\">',
            '<span>' + metric["MetricName"] + '</span>',
            render_value,
            '</div>'
        ]

        console.log(arr)
        return arr.join('')
    }



    function update() {
        console.log(realtime)
        if (realtime === 'on')
            get_sources()

        setTimeout(update, updateInterval)

    }

    function get_sources(){
        var apiURL_query_sources = "http://192.168.0.102:5000/api/sources"
        $.ajax({
            url: apiURL_query_sources, success: function (sources) {
                console.log(sources)
                for (var i = 0; i < sources.length; i++) {
                    for(var j = 0; j< sources[i]['metrics'].length; j++){
                        var metric = sources[i]['metrics'][j]
                        if (metric['MetricDomain'] == 'switch'){
                            if (metric['DataPoint']['Value'] == 'on'){
                                $('#'+metric['MetricId']).find("input").prop('checked', true)
                            }else{
                                $('#'+metric['MetricId']).find("input").prop('checked', false)
                            }

                        }else{
                            $('#'+metric['MetricId'] + " span:nth-child(2)").text(metric['DataPoint']['Value'])

                        }
                    }
                }
            }
        });
    }




    $('body').on('click', '.round', function (){
        console.log($(this).prev().prop( "checked"))
        var new_value = ""
        if($(this).prev().prop( "checked")){
            // now_value is on
            new_value = 'off'
        }else{
            new_value = 'on'
        }

        var metric_id = $(this).closest('div').attr('id');
        var source_id = $(this).closest('div').parent().attr('id');
        data_post = {
            "header":{},
            "body":{
                "SourceId": source_id,
                "MetricId": metric_id,
                "new_value": new_value
            }
        }
        console.log(data_post)
        $.ajax({
            url: "http://192.168.0.102:5000/api/metric",
            type: 'post',
            dataType: 'json',
            contentType: 'application/json',
            data: JSON.stringify(data_post)
        });

    });

    $("#dashboard").trigger("click");
    update()

    $("#form_add_platform").submit(function (e) {
        e.preventDefault();
        // Coding
        data_post = {
            "ssh_host": $('#input_host').val(),
            "user_name": $('#input_user_name').val(),
            "password": $('#input_password').val(),
            "platform_host": $('#input_platform_host').val(),
            "platform_port": $('#input_platform_port').val(),
            "platform_name": $('#input_platform_name').val(),
            "platform_type": $('#input_platform_type option:selected').text(),
            "broker_fog": $('#input_broker_fog').val()
        }
        console.log(data_post)
        $.ajax({
            url: "http://192.168.0.102:5000/api/platform",
            type: 'post',
            dataType: 'json',
            contentType: 'application/json',
            success: function (data) {
                console.log("data: " + JSON.stringify(data))
                $('#input_host').val("")
                $('#input_user_name').val("")
                $('#input_password').val("")
                $('#input_platform_host').val("")
                $('#input_platform_port').val("")
                $('#input_platform_name').val("")
                $('#input_broker_fog').val("")
                if ('error' in data){
                    document.getElementById("alert_errror").style.display = "block";
                    setTimeout(timeout_alert, 2000, "alert_errror")
                    // realtime = 'on'

                }else{
                    document.getElementById("alert_sucess").style.display = "block";
                    setTimeout(timeout_alert, 2000, "alert_sucess")
                    setTimeout(query_api, updateInterval)
                }

            },
            data: JSON.stringify(data_post)
        });

        // realtime = 'off'
        document.getElementById("alert_addding").style.display = "block";
        $('#alert_adding').hide().fadeIn('slow')
        setTimeout(timeout_alert, 2000, "alert_addding")
        $("#add_platform").modal('toggle');

    });

    function timeout_alert(alert_id){
        document.getElementById(alert_id).style.display = "none";
    }

});
