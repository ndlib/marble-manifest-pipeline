exports.iterator = function iterator (event, context, callback) {
  let images = event.data.iterator.images
  console.log(event.data.iterator.images)

  let imageToProcess = images.shift()

  event.data.iterator.imageToProcess = imageToProcess
  event.data.iterator.continue = imageToProcess !== undefined
  callback(null, event.data)
}