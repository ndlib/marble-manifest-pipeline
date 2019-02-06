const sharp = require('sharp')
const AWS = require('aws-sdk')

exports.processor = async (event, context, callback) => {
//async function foo() {
  const eventId = event["config"]["process-bucket-write-basepath"]
    + "/" + event["id"] + "/"
  const imagesFolder = 'images/'
  try {
    const img = event.pgimage.iterator.imageToProcess
    const getParams = {
      Bucket: event["config"]["process-bucket"],
      Key: img
    }
    const s3 = new AWS.S3()
    const response = await s3.getObject(getParams).promise()
    const processedImg = await sharp(response.Body)
                                .limitInputPixels(false)
                                .resize(2000, 2000, { withoutEnlargement: true, fit: 'inside' })
                                .tiff({
                                  compression: 'deflate',
                                  pyramid: true,
                                  tile: true
                                })
                                .toBuffer()

    // turn /process/<id>/images/<img>.<ext> to <img>
    let imageName = img.split('/').pop()
    imageName = imageName.substr(0, imageName.lastIndexOf('.'))
    const putParams = {
      Body: processedImg,
      Bucket: event["config"]["process-bucket"],
      Key: eventId + imagesFolder + imageName + '.tif'
    }
    await s3.putObject(putParams).promise()
  } catch (err) {
    if(!('errors' in event)) {
      event.errors = []
    }
    const ruhroh = { 'image': event.id, 'error': err.message }
    event.errors.push(ruhroh)
    console.error(err, err.message)
  }

  callback(null, event)
}

//foo()
