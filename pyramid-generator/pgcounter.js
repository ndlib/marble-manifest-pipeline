const AWS = require('aws-sdk')

exports.counter = async (event, context, callback) => {
//async function foo() {
  const eventId = event["config"]["process-bucket-read-basepath"]
    + "/" + event["id"] + "/"
  const imagesFolder = 'images/'
  let images = []
  try {
    /*
      * Prefix: only search for this event folder
      * StartAfter: do not include this events 'folder' in the results
    */
    let params = {
      Bucket: event["config"]["process-bucket"],
      Prefix: eventId + imagesFolder,
      Delimiter: '/', 
      StartAfter: eventId + imagesFolder,
    }

    const s3 = new AWS.S3()
    let attempt = 0
    while(params.ContinuationToken || attempt === 0) {
      attempt++
      const objs = await s3.listObjectsV2(params).promise()
      objs.Contents.forEach(function (content) {
        //images.push(content.Key.replace(eventId + imagesFolder,''))
        images.push(content.Key)
      })
      if(objs.IsTruncated) {
        params.ContinuationToken = objs.NextContinuationToken
      } else {
        delete params.ContinuationToken
      }
    }
  } catch(err) {
    console.log(err, err.message)
  }

  event.pgimage = { "iterator": { "images": images } }
  //event.pgimage.iterator = { "images": images }
  callback(null, event)
}

//foo()