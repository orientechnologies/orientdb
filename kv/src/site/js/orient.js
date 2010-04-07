var angle = 0;
var speed = -0.2;
var acceleration = 0;
var homepage = "http://www.orientechnologies.com"; 

function orient_init(){
  orient_start_sun_animation();
}

function orient_start_sun_animation()
{
  if( navigator.appName == "Microsoft Internet Explorer"){
    var img = document.getElementById("logo_sun");
    img.style.left = '-10px';
    img.style.top = '-5px';
    $('#logo_sun').bind("click", orient_home );
    return;
  }

  var rot = $('#logo_sun').rotate( {"angle":angle,
       bind:
	      [
		     {"mouseover":function(){
           acceleration = +0.2;
         }},
		     {"mouseout":function(){
           acceleration = -0.1;
           }},
		     {"click":orient_home}
	      ]}
    );

  if( speed < -0.2 )
    speed = -0.2;
  else if( speed > 4 )
    speed = 4;
  else
    speed += acceleration;

  angle += speed;
  
  if( angle > 359)
    angle = 0;
  if( angle < 0)
    angle = 359;
  
	setTimeout(orient_start_sun_animation, 25);
}

function orient_home(){
  location.href = homepage;
}

function orient_google_stats(){
  var gaJsHost = (("https:" == document.location.protocol) ? "https://ssl." : "http://www.");
  document.write(unescape("%3Cscript src='" + gaJsHost + "google-analytics.com/ga.js' type='text/javascript'%3E%3C/script%3E"));

  try {
    var pageTracker = _gat._getTracker("UA-12202293-3");
    pageTracker._setDomainName(".orientechnologies.com");
    pageTracker._trackPageview();
  } catch(err) {}
}
