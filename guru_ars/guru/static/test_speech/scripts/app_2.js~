
function __log(e, data) {
    console.log( "\n" + e + " " + (data || ''));
  }
  var audio_context;
  var recorder;
  function startUserMedia(stream) {
    var input = audio_context.createMediaStreamSource(stream);
    __log('Media stream created.');
    // Uncomment if you want the audio to feedback directly
    // input.connect(audio_context.destination);
    //__log('Input connected to audio context destination.');
    
    recorder = new Recorder(input);
    __log('Recorder initialised.');
  }
  function startRecording(button) {
    recorder && recorder.record();
    button.disabled = true;
    button.nextElementSibling.disabled = false;
    __log('Recording...');
  }
  function stopRecording(button) {
    recorder && recorder.stop();
    button.disabled = true;
    button.previousElementSibling.disabled = false;
    __log('Stopped recording.');
    
    // create WAV download link using audio data blob
    //createDownloadLink();
    sendAudio();
    recorder.clear();
  }

function sendAudio(){
console.log("inside send audio");
recorder && recorder.exportWAV(function(blob) {

      console.log("blob");
      console.log(blob);
      var audioURL = window.URL.createObjectURL(blob);
      audio.src = audioURL;
      console.log("audio.src");
      console.log(audio.src)

	var reader = new window.FileReader();
 	reader.readAsDataURL(blob); 
 	reader.onloadend = function() {
        base64data = reader.result;                
        console.log(base64data);
        }
});
}


  function createDownloadLink() {
    recorder && recorder.exportWAV(function(blob) {
      var url = URL.createObjectURL(blob);
      var li = document.createElement('li');
      var au = document.createElement('audio');
      var hf = document.createElement('a');
      
      au.controls = true;
      au.src = url;
      hf.href = url;
      hf.download = new Date().toISOString() + '.wav';
      hf.innerHTML = hf.download;
      li.appendChild(au);
      li.appendChild(hf);
      recordingslist.appendChild(li);
    });
  }



  window.onload = function init() {
    try {
      // webkit shim
      window.AudioContext = window.AudioContext || window.webkitAudioContext;
      navigator.getUserMedia = navigator.getUserMedia || navigator.webkitGetUserMedia;
      window.URL = window.URL || window.webkitURL;
      
      audio_context = new AudioContext;
      __log('Audio context set up.');
      __log('navigator.getUserMedia ' + (navigator.getUserMedia ? 'available.' : 'not present!'));
    } catch (e) {
      alert('No web audio support in this browser!');
    }
    
    navigator.getUserMedia({audio: true}, startUserMedia, function(e) {
      __log('No live audio input: ' + e);
    });
  };





//var canvasCtx = canvas.getContext("2d");

//main block for doing the audio recording
/*
if (navigator.getUserMedia) {
  console.log('getUserMedia supported.');

  var constraints = { audio: true };
  var chunks = [];

  var onSuccess = function(stream) {
   var mediaRecorder = new MediaRecorder(stream);
   console.log(MediaRecorder.isTypeSupported("audio/ogg"));
   console.log(MediaRecorder.isTypeSupported("audio/ogg;codecs=opus"));
   console.log(MediaRecorder.isTypeSupported("audio/ogg;codecs=vorbis"));
   console.log(MediaRecorder.isTypeSupported("audio/webm"));
   console.log(MediaRecorder.isTypeSupported("audio/webm;codecs=opus"));
   console.log(MediaRecorder.isTypeSupported("audio/webm;codecs=vorbis"));

    record.onclick = function() {
      mediaRecorder.start();
      console.log(mediaRecorder.state);
      console.log("recorder started");
      record.style.background = "red";

      stop.disabled = false;
      record.disabled = true;
    }

    stop.onclick = function() {
      mediaRecorder.stop();
      console.log(mediaRecorder.state);
      console.log("recorder stopped");
      record.style.background = "";
      record.style.color = "";
      // mediaRecorder.requestData();

      stop.disabled = true;
      record.disabled = false;
    }

    mediaRecorder.onstop = function(e) {
      console.log("data available after MediaRecorder.stop() called.");

      //var clipName = prompt('Enter a name for your sound clip?','My unnamed clip');
      //console.log(clipName);
      //var clipContainer = document.createElement('article');
      //var clipLabel = document.createElement('p');
      var audio = document.createElement('audio');
   
      //var deleteButton = document.createElement('button');
     
      //clipContainer.classList.add('clip');
      //audio.setAttribute('controls', '');
      //deleteButton.textContent = 'Delete';
      //deleteButton.className = 'delete';

      //if(clipName === null) {
      //  clipLabel.textContent = 'My unnamed clip';
      //} else {
       // clipLabel.textContent = clipName;
      //}

      //clipContainer.appendChild(audio);
      //clipContainer.appendChild(clipLabel);
      //clipContainer.appendChild(deleteButton);
      //soundClips.appendChild(clipContainer);

      //audio.controls = true;
      var blob = new Blob(chunks, { 'type' : 'audio/ogg;codecs=opus' });
      chunks = [];
      var audioURL = window.URL.createObjectURL(blob);
      audio.src = audioURL;
      console.log("recorder stopped");
      console.log("audio.src")
      console.log(audio.src)

	var reader = new window.FileReader();
 	reader.readAsDataURL(blob); 
 	reader.onloadend = function() {
        base64data = reader.result;                
        console.log(base64data);

	$.ajax({
                    url: 'http://guru.headrun.com:8010/speech',
                    data: {'sound_data': base64data},
                    type: 'POST',
                    dataType: 'text',
                    success: function(response){
			var data = $.parseJSON(response)
                        console.log(data);
			if (data.result){
				input_form.value = data.result;}
			else{
                               console.log('no results');}

			
                    }
                });
  }
      //deleteButton.onclick = function(e) {
      //  evtTgt = e.target;
      //  evtTgt.parentNode.parentNode.removeChild(evtTgt.parentNode);
      //}

      //clipLabel.onclick = function() {
        //var existingName = clipLabel.textContent;
        //var newClipName = prompt('Enter a new name for your sound clip?');
        //if(newClipName === null) {
        //  clipLabel.textContent = existingName;
        //} else {
         // clipLabel.textContent = newClipName;
        //}
      //}
    }

    mediaRecorder.ondataavailable = function(e) {
	console.log(e)
      chunks.push(e.data);
    }
  }

  var onError = function(err) {
    console.log('The following error occured: ' + err);
  }

  navigator.getUserMedia(constraints, onSuccess, onError);
} else {
   console.log('getUserMedia not supported on your browser!');
}
*/

