/**
 * @fileoverview Demo of Cloud Speech API. Users can submit audio and the API
 * will transcribe it for them. Requires the recorderjs library to capture audio
 * input from the microphone.
 */

'use strict';
var cloud = window.cloud || {};

/**
 * Constructor for Speech Demo.
 * @constructor
 */
cloud.Speech = function() {
/** Constants **/

  /**
   * Speech API key.
   * @const {string}
   */
  this.API_KEY = 'AIzaSyDcB0vAivytvdjIlUz_qTqn2xc7jLPvTww';

  /**
   * Service URL to request annotations.
   * @const {string}
   */
  this.SERVICE_URL =
      'https://speech.googleapis.com/v1beta1/speech:syncrecognize?key=' +
      this.API_KEY;

  /**
   * Maximum time, in miliseconds allowed to record audio.
   * @const {number}
   */
  this.MAX_RECORD_TIME = 30000;

  /**
   * Maximum time, in miliseconds to wait for a server response before showing
   *     an error message.
   * @const {number}
   */
  this.TIMEOUT_AMOUNT = 60000;

  /**
   * Time, in miliseconds to keep recording after user hits the stop button.
   * @const {number}
   */
  this.RECORD_END_DELAY = 500;

  /**
   * Number of submits allowed before a Captcha is shown.
   * @const {number}
   */
  this.NO_CAPTCHA_SUBMITS_ALLOWED = 5;

  /** Elements **/

  /**
   * Clickable button used to start and stop audio recording.
   * @private {!Element|null}
   */
  this.recordButton_ = document.getElementById('speech_demo_record_button');

  /**
   * Processing caption inside the record button.
   * @private {!Element|null}
   */
  this.processingCaption_ = document.getElementById(
      'speech_demo_record_processing');

  /**
   * Time display caption inside the record button.
   * @private {!Element|null}
   */
  this.timerCaption_ = document.getElementById(
      'speech_demo_record_timer');

  /**
   * DOM container for audio recorder.
   * @private {!Element|null}
   */
  this.recordContainer_ = document.getElementById('speech_demo_section');

  /**
   * Drop down element that contains the different languages accepted by the
   *     API.
   * @private {!Element|null}
   */
  this.recordLanguageSelect_ =  document.getElementById(
      'speech_demo_record_language');
  

  /**
   * DOM container that will show the results sent back by the API.
   * @private {!Element|null}
   */
  this.resultContainer_ = document.getElementById(
      'speech_demo_results_container');

  /**
   * DOM element for the text transcription result.
   * @private {!Element|null}
   */
  this.result_ = document.getElementById('speech_demo_results');

  /**
   * DOM element for the JSON result.
   * @private {!Element|null}
   */
  this.resultCode_ = document.getElementById('speech_demo_results_code');

  /**
   * Container element for captcha.
   * @private {!Element|null}
   */
  this.captchaContainer_ = document.getElementById(
      'speech_demo_captcha_container');

  /**
   * Container element for error messages.
   * @private {!Element|null}
   */
  this.errorContainer_ = document.getElementById('speech_demo_error_container');

  /**
   * Container element for error messages when a mic is not available.
   * @private {!Element|null}
   */
  this.noMicContainer_ = document.getElementById('speech_demo_unavailable');

  /** Properties **/

  /**
   * Track if the application is currently recording.
   * @private {boolean}
   */
  this.isRecording_ = false;

  /**
   * Track if the application is currently processing a file.
   * @private {boolean}
   */
  this.isProcessing_ = false;

  /**
   * ID used to track whether a submit has timed out.
   * @private {number}
   */
  this.submitTimeout_ = 0;

  /**
   * Track the time when a recording is started.
   * @private {number}
   */
  this.startTime_ = 0;

  /**
   * Track the number of times audio is submitted.
   * @private {number}
   */
  this.totalSubmits_ = 0;

  /**
   * Reference to browser's audio context.
   * @private {*}
   */
  this.audioContext_ = null;

  /**
   * Recordjs instance.
   * @private {*}
   */
  this.rec_ = null;
};


/**
 * Initializes the demo.
 */
cloud.Speech.prototype.init = function(){
  // Check to see if the browser supports the file reader API.
  if(this.supportsFileReader()) {
    // Add event listeners.
    this.recordButton_.addEventListener('click', function(e){
      e.preventDefault();
      // Initialize audio if we don't have an audio context
      if(!this.audioContext_) {
        this.initRecorder();
      } else {
        this.toggleRecord();
      }
    }.bind(this));
  } else {
    this.hideDemo();
  }
};


/**
 * Initializes the audio recorder, using recordjs library.
 */
cloud.Speech.prototype.initRecorder = function(){
  try {
    // Webkit shim.
    window.AudioContext = window.AudioContext ||  window.webkitAudioContext;
    navigator.getUserMedia = navigator.getUserMedia ||
        navigator.webkitGetUserMedia ||
        navigator.mozGetUserMedia ||
        navigator.msGetUserMedia;
    window.URL = window.URL || window.webkitURL;
    this.audioContext_ = new AudioContext;
  } catch (e) {
    // If there was an error setting up the recorder, hide the demo.
    this.hideDemo();
  }
  navigator.getUserMedia(
      {audio: true},
      function(stream){
        this.startUserMedia(stream);
      }.bind(this),
      function(e) {
        // Recorder is not available.
        this.hideDemo();
      }.bind(this));
};


/*
 * Hides the demo, used when a feature is not available on the browser.
 */
cloud.Speech.prototype.hideDemo = function(){
  this.recordContainer_.style.display = 'none';
  this.noMicContainer_.style.display = 'block';
  document.getElementById('speech_demo_reload').addEventListener('click',
      function(e){
        e.preventDefault();
        location.reload();
  });
};


/**
 * Processes the result of an audio record to get it ready to send to the API.
 * @param {!File} file Audio recording file.
 */
cloud.Speech.prototype.processAudioRecording = function(file) {
  var currentLanguage = this.recordLanguageSelect_.children[
      this.recordLanguageSelect_.selectedIndex].value;
  var reader = new FileReader();
  // Once the file is loaded, send it to the API as a binary string.
  reader.onload = function(readerEvt) {
      var binaryString = readerEvt.target.result;
      this.sendAudio(btoa(binaryString), currentLanguage, 'LINEAR16',
          this.audioContext_.sampleRate);
  }.bind(this);
  reader.readAsBinaryString(file);
};


/*
 * Disables all form elements while form is processing.
 */
cloud.Speech.prototype.disableForm = function() {
  this.recordLanguageSelect_.setAttribute('disabled','true');
  this.recordButton_.setAttribute('disabled','true');
};


/*
 * Enables all form elements.
 */
cloud.Speech.prototype.enableForm = function() {
  this.recordLanguageSelect_.removeAttribute('disabled');
  this.recordButton_.removeAttribute('disabled');
};


/**
 * Sends the audio data to the speech API.
 * @param {string} binaryAudioFile Base64 encoded audio file.
 * @param {string} language Language that the audio is in.
 * @param {string} encoding Type of file that is being sent through.
 * @param {number} sampleRate Sample rate, in Hz of audio.
 */
cloud.Speech.prototype.sendAudio = function(binaryAudioFile, language, encoding,
    sampleRate){
  // Build JSON request.
  var request = JSON.stringify({
    config: {
      encoding: encoding,
      sampleRate: sampleRate,
      languageCode: language,
      maxAlternatives: 1
    },
    audio: {
      content: binaryAudioFile
    }
  });

  // Post Request.
  var ajax = new XMLHttpRequest();
  ajax.onload = function(e) {
    if (ajax.status >= 200 && ajax.status < 400) {
      var data = JSON.parse(ajax.responseText);
      this.showResults(data);
    } else {
      this.handleError(e);
    }
  }.bind(this);
  ajax.onerror = this.handleError;
  ajax.open('POST', this.SERVICE_URL, true);
  ajax.send(request);
  this.requestTimedOut = false;
  this.submitTimeout_ = setTimeout(function(){
    this.showError();
    this.requestTimedOut = true;
  }.bind(this), this.TIMEOUT_AMOUNT);
};


/**
 * Handles an AJAX error when communicating with the API.
 * @param {string} error
 */
cloud.Speech.prototype.handleError = function(error) {
  if(!this.requestTimedOut){
    clearTimeout(this.submitTimeout_);
    this.enableForm();
    this.isProcessing_ = false;
    this.errorContainer_.style.display = 'none';
    console.log(error);
  }
};


/**
 * Shows error message.
 */
cloud.Speech.prototype.showError = function() {
  clearTimeout(this.submitTimeout_);
  this.enableForm();
  this.isProcessing_ = false;
  this.setButtonState('');
  this.errorContainer_.style.display = 'block';
};


/**
 * Shows the results of a successful API call.
 * @param {*} data Result of the API call.
 */
cloud.Speech.prototype.showResults = function(data){
  if (this.requestTimedOut) {
    return;
  }
  clearTimeout(this.submitTimeout_);
  // Format the response so that it can be displayed in a <pre> tag.
  this.enableForm();
  this.isProcessing_ = false;
  this.resultContainer_.style.display = 'block';
  this.setButtonState('');
  this.resultCode_.textContent = JSON.stringify(data, null, 2);
  if (!data.results) {
    this.result_.textContent = 'No speech detected';
    return;
  }
  var transcript = data.results.map(function(result) {
    return result.alternatives[0].transcript;
  });
  this.result_.textContent = transcript.join('');
};


/**
 * Toggles recording mode on and off depending on current state.
 */
cloud.Speech.prototype.toggleRecord = function(){
  if(this.rec_){
    if(this.isRecording_) {
      this.stopRecording();
    } else if(!this.isProcessing_) {
      this.startRecording();
    }
  } else {
    this.hideDemo();
  }
};


/**
 * Starts audio recording.
 */
cloud.Speech.prototype.startRecording = function(){
  var timerUpdate;
  var recTime = 0;
  var formattedTime = '00';
  var totalTime = ' / 00:' + this.MAX_RECORD_TIME.toString().slice(0,-3);
  this.resultContainer_.style.display = 'none';
  this.errorContainer_.style.display = 'none';
  this.setButtonState('recording');
  this.timerCaption_.textContent = '00:00' + totalTime;
  this.startTime_ = Date.now();
  this.rec_.clear();
  this.rec_.record();
  this.isRecording_ = true;
  // Monitor the amount of time recorded.
  timerUpdate = setInterval(function(){
    if(!this.isRecording_) {
      clearInterval(timerUpdate);
    } else {
      recTime = Date.now() - this.startTime_;
      if(recTime >= this.MAX_RECORD_TIME) {
        this.stopRecording();
        clearInterval(timerUpdate);
      } else if(recTime >= 1000) {
        formattedTime = recTime < 10000 ?
            '0' + recTime.toString().slice(0,-3) :
            recTime.toString().slice(0,-3);
        this.timerCaption_.textContent = '00:' + formattedTime + totalTime;
      }
    }
  }.bind(this),250);
};


/**
 * Stops audio recording.
 */
cloud.Speech.prototype.stopRecording = function(){
  //Update the UI immediately on a stop command.
  this.isProcessing_ = true;
  this.isRecording_ = false;
  this.setButtonState('processing');
  // But delay the actual stopping of recording 500ms to add a small buffer.
  setTimeout(function(){
    this.rec_.stop();
    // Bypass captcha if it's not needed.
    if(!this.needsCaptcha()) {
      this.totalSubmits_++;
      this.captchaSuccess();
    } else {
      this.totalSubmits_ = 0;
      this.captchaContainer_.style.display = 'block';
    }
  }.bind(this), this.RECORD_END_DELAY);
};


/**
 * Updates state for record button.
 * @param {string} state New state for record button.
 */
cloud.Speech.prototype.setButtonState = function(state) {
  switch(state) {
    case 'processing':
      this.recordButton_.classList.remove('recording');
      this.recordButton_.classList.add('processing');
      this.processingCaption_.style.display = 'block';
      this.timerCaption_.style.display = 'none';
      break;
    case 'recording':
      this.recordButton_.classList.add('recording');
      this.recordButton_.classList.remove('processing');
      this.processingCaption_.style.display = 'none';
      this.timerCaption_.style.display = 'block';
      break;
    default:
      this.recordButton_.classList.remove('recording');
      this.recordButton_.classList.remove('processing');
      this.processingCaption_.style.display = 'none';
      this.timerCaption_.style.display = 'none';
      break;
  }
};


/**
 * Handles a successful captcha completion.
 */
cloud.Speech.prototype.captchaSuccess = function(){
  this.captchaContainer_.style.display = 'none';
  this.rec_.exportWAV(function(blob){
    this.processAudioRecording(blob);
  }.bind(this));
};


/**
 * Checks if we need to show a captcha.
 * @return {boolean}
 */
cloud.Speech.prototype.needsCaptcha = function(){
  return (this.totalSubmits_ > this.NO_CAPTCHA_SUBMITS_ALLOWED);
};


/**
 * Sets up Recorder.js library.
 */
cloud.Speech.prototype.startUserMedia = function(stream) {
  var input = this.audioContext_.createMediaStreamSource(stream);
  this.rec_ = new window.Recorder(input,{
    numChannels: 1,
    workerPath: './scripts/recorderWorker-bundle.js'
  });
  // Once set up, send a "toggle record" to start initial record
  this.toggleRecord();
};


/**
 * Checks for file reader support.
 * @return {boolean}
 */
cloud.Speech.prototype.supportsFileReader = function() {
  return 'FileReader' in window;
};


/**
 * Handles a global Recaptcha success event.
 */
window.globalCaptchaSuccess = function(){
  cloud.speechDemo.captchaSuccess();
};


cloud.speechDemo = new cloud.Speech();
cloud.speechDemo.init();
