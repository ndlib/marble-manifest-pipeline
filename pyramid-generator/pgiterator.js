exports.iterator = function iterator (event, context, callback) {
  let images = event.pgimage.iterator.images
  let imageToProcess = images.shift()

  event.pgimage.iterator.imageToProcess = imageToProcess
  event.pgimage.iterator.continue = imageToProcess !== undefined
  callback(null, event)
}