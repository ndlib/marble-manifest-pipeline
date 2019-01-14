const sharp = require('sharp')
const AWS = require('aws-sdk')

exports.processor = async (event, context, callback) => {
//async function foo() {
  const eventId = event["data"]["config"]["process-bucket-write-basepath"]
   + "/" + event["data"]["unique-identifier"] + "/"
  const imagesFolder = 'images/'
  try {
    const img = event.data.iterator.imageToProcess
    const getParams = {
      Bucket: process.env.PROCESS_BUCKET,
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
      Bucket: process.env.PROCESS_BUCKET,
      Key: eventId + imagesFolder + imageName + '.tif'
    }
    await s3.putObject(putParams).promise()
  } catch (err) {
    const ruhroh = { 'image': event.id, 'error': err.message }
    event.data.errors.push(ruhroh)
    console.error(err, err.message)
  }

  callback(null, event.data)
}

//foo()
