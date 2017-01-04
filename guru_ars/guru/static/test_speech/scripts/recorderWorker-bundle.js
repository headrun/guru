//third_party/javascript/recorderjs/recorderWorker.js
/**
 * @license
 * The MIT License (MIT)
 * Copyright © 2013 Matt Diamond
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
var recLength = 0,
  recBuffers = [],
  sampleRate,
  numChannels,
  clearBetweenExports;

this.onmessage = function(e){
  switch(e.data.command){
    case 'init':
      init(e.data.config);
      break;
    case 'record':
      record(e.data.buffer);
      break;
    case 'exportWAV':
      exportWAV(e.data.type, e.data.sequenceNumber);
      break;
    case 'getBuffer':
      getBuffer();
      break;
    case 'clear':
      clear();
      break;
  }
};

function init(config){
  sampleRate = config.sampleRate;
  numChannels = config.numChannels;
  clearBetweenExports = config.clearBetweenExports;
  initBuffers();
}

function record(inputBuffer){
  for (var channel = 0; channel < numChannels; channel++){
    recBuffers[channel].push(inputBuffer[channel]);
  }
  recLength += inputBuffer[0].length;
}

function exportWAV(type, sequenceNumber){
  var buffers = [];
  for (var channel = 0; channel < numChannels; channel++){
    buffers.push(mergeBuffers(recBuffers[channel], recLength));
  }
  // Clear so that we can call exportWAV multiple times.
  if (clearBetweenExports) {
    clear();
  }
  if (numChannels === 2){
      var interleaved = interleave(buffers[0], buffers[1]);
  } else {
      var interleaved = buffers[0];
  }
  var dataview = encodeWAV(interleaved, sequenceNumber);
  var audioBlob = new Blob([dataview], { type: type });

  this.postMessage(audioBlob);
}

function getBuffer(){
  var buffers = [];
  for (var channel = 0; channel < numChannels; channel++){
    buffers.push(mergeBuffers(recBuffers[channel], recLength));
  }
  this.postMessage(buffers);
}

function clear(){
  recLength = 0;
  recBuffers = [];
  initBuffers();
}

function initBuffers(){
  for (var channel = 0; channel < numChannels; channel++){
    recBuffers[channel] = [];
  }
}

function mergeBuffers(recBuffers, recLength){
  var result = new Float32Array(recLength);
  var offset = 0;
  for (var i = 0; i < recBuffers.length; i++){
    result.set(recBuffers[i], offset);
    offset += recBuffers[i].length;
  }
  return result;
}

function interleave(inputL, inputR){
  var length = inputL.length + inputR.length;
  var result = new Float32Array(length);

  var index = 0,
    inputIndex = 0;

  while (index < length){
    result[index++] = inputL[inputIndex];
    result[index++] = inputR[inputIndex];
    inputIndex++;
  }
  return result;
}

function floatTo16BitPCM(output, offset, input){
  for (var i = 0; i < input.length; i++, offset+=2){
    var s = Math.max(-1, Math.min(1, input[i]));
    output.setInt16(offset, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
  }
}

function writeString(view, offset, string){
  for (var i = 0; i < string.length; i++){
    view.setUint8(offset + i, string.charCodeAt(i));
  }
}

function encodeWAV(samples, sequenceNumber){
  var bufferSize = 44 + samples.length * 2;
  var sequenceChunkSize = 0;
  if (sequenceNumber) {
    sequenceChunkSize = 12;
    bufferSize += sequenceChunkSize;
  }
  var view = new DataView(new ArrayBuffer(bufferSize));

  /* RIFF identifier */
  writeString(view, 0, 'RIFF');
  /* RIFF chunk length */
  view.setUint32(4, 36 + samples.length * 2, true);
  /* RIFF type */
  writeString(view, 8, 'WAVE');
  /* format chunk identifier */
  writeString(view, 12, 'fmt ');
  /* format chunk length */
  view.setUint32(16, 16, true);
  /* sample format (raw) */
  view.setUint16(20, 1, true);
  /* channel count */
  view.setUint16(22, numChannels, true);
  /* sample rate */
  view.setUint32(24, sampleRate, true);
  /* byte rate (sample rate * block align) */
  view.setUint32(28, sampleRate * numChannels * 2, true);
  /* block align (channel count * bytes per sample) */
  view.setUint16(32, numChannels * 2, true);
  /* bits per sample */
  view.setUint16(34, 16, true);
  if (sequenceNumber) {
    /* to put in sequence # TODO(ardubey): maybe put in Json container? */
    writeString(view, 36, 'seqn');
    /* seqn chunk length */
    view.setUint32(40, 4, true);
    /* sequence number */
    view.setUint32(44, sequenceNumber, true);
  }

  /* data chunk identifier */
  writeString(view, 36 + sequenceChunkSize, 'data');
  /* data chunk length */
  view.setUint32(40 + sequenceChunkSize, samples.length * 2, true);

  floatTo16BitPCM(view, 44 + sequenceChunkSize, samples);

  return view;
}

