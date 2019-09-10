from pyvips import Image
import os


file = '/Users/jhartzle/Desktop/pytif_images_20190710/one.tif'

filebase = os.path.basename('../example/images/WIN710-1-016.tif')
filename, file_ext = os.path.splitext(filebase)
tif_file = "pyrimid-sixth.tif"


def items_json(file, label):
    return '''{
          "id": "https://manifest-pipeline-v3.libraries.nd.edu/testsize/canvas/testsize2%2F~file~",
          "type": "Canvas",
          "label": "~label~",
          "height": 2442,
          "width": 1755,
          "thumbnail": [
            {
              "id": "https://gf58vp8mjh.execute-api.us-east-1.amazonaws.com/latest/iiif/2/testsize2%2F~file~/full/250,/0/default.jpg",
              "type": "Image",
              "service": [
                {
                    "id": "https://gf58vp8mjh.execute-api.us-east-1.amazonaws.com/latest/iiif/2/testsize2%2F~file~",
                  "type": "ImageService2",
                  "profile": "http://iiif.io/api/image/2/level2.json"
                }
              ]
            }
          ],
          "items": [ {
              "id": "AnnotationUrl",
              "type": "AnnotationPage",
              "items": [
                {
                    "id": "https://gf58vp8mjh.execute-api.us-east-1.amazonaws.com/latest/iiif/2/testsize2%2F~file~",
                  "type": "Annotation",
                  "motivation": "painting",
                    "target": "https://manifest-pipeline-v3.libraries.nd.edu/testsize2/canvas/testsize2%2F~file~",
                  "body": {
                    "id": "https://gf58vp8mjh.execute-api.us-east-1.amazonaws.com/latest/iiif/2/testsize2%2F25mb/full/full/0/default.jpg",
                    "type": "Image",
                    "format": "image/jpeg",
                    "service": [
                      {
                        "id": "https://gf58vp8mjh.execute-api.us-east-1.amazonaws.com/latest/iiif/2/testsize2%2F~file~",
                        "type": "ImageService2",
                        "profile": "http://iiif.io/api/image/2/level2.json"
                      }
                    ]
                  }
                }
              ]
            }
          ]
        }'''.replace('~file~', file).replace('~label~', label)


def generate_tiff(file, tif_basename, resize, tile_size, compression):
    tif_basename = os.path.join('./images', tif_basename)
    try:
        print 'Generating pyramid tif for: {}'.format(tif_basename)
        image = Image.new_from_file(file, access='sequential')
        if (resize != 1):
            image = image.resize(resize)
        image.tiffsave(tif_basename + '.tif', tile=True, tile_width=tile_size, tile_height=tile_size, pyramid=True, compression=compression)
        return tif_basename + " | " + str(image.width) + " x " + str(image.height) + " | " + str(image.xres) + " x " + str(image.yres) + " pixels / mm | compression: " + compression + " | tiled at " + str(tile_size)

    except Exception as e:
        print(str(e))


items = []
file = '../example/images/WIN710-1-016.tif'
label = generate_tiff(file, 'winkler_compressed', 1, 512, 'deflate')
items.append(items_json('winkler_compressed', label))
label = generate_tiff(file, 'winkler_uncompressed', 1, 512, 'none')
items.append(items_json('winkler_uncompressed', label))

file = '/Users/jhartzle/Desktop/pytif_images_20190710/one.tif'

label = generate_tiff(file, 'map_fourth_compressed', .25, 512, 'deflate')
items.append(items_json('map_fourth_compressed', label))
label = generate_tiff(file, 'map_fourth_uncompressed', .25, 512, 'none')
items.append(items_json('map_fourth_uncompressed', label))

label = generate_tiff(file, 'map_fifth_compressed', .20, 512, 'deflate')
items.append(items_json('map_fifth_compressed', label))
label = generate_tiff(file, 'map_fifth_uncompressed', .20, 512, 'none')
items.append(items_json('map_fifth_uncompressed', label))

label = generate_tiff(file, 'map_sixth_compressed', .15, 512, 'deflate')
items.append(items_json('map_sixth_compressed', label))
label = generate_tiff(file, 'map_sixth_uncompressed', .15, 512, 'none')
items.append(items_json('map_sixth_uncompressed', label))

label = generate_tiff(file, 'map_third_compressed', .33, 512, 'deflate')
items.append(items_json('map_third_compressed', label))
label = generate_tiff(file, 'map_third_uncompressed', .33, 512, 'none')
items.append(items_json('map_third_uncompressed', label))


f = open("index.json", "w+")
f.write('''
{
  "@context": [
    "http://www.w3.org/ns/anno.jsonld",
    "http://iiif.io/api/presentation/3/context.json"
  ],
  "type": "Manifest",
  "id": "https://manifest-pipeline-v3.libraries.nd.edu/testsize2/manifest",
  "label": {
    "en": [
      "Test Sizes"
    ]
  },
  "metadata": [
    {
      "label": {
        "en": [
          "Summary"
        ]
      },
      "value": {
        "en": [
          "The big test of sizes"
        ]
      }
    }
  ],
  "rights": "Publica",
  "requiredStatement": {
    "label": {
      "en": [
        "Attribution"
      ]
    },
    "value": {
      "en": [
        "People"
      ]
    }
  },
  "viewingDirection": "left-to-right",
  "thumbnail": [
    {
      "id": "https://gf58vp8mjh.execute-api.us-east-1.amazonaws.com/latest/iiif/2/testsize2%2F25mb/full/250,/0/default.jpg",
      "type": "Image",
      "service": [
        {
            "id": "https://gf58vp8mjh.execute-api.us-east-1.amazonaws.com/latest/iiif/2/testsize2%2F25mb",
          "type": "ImageService2",
          "profile": "http://iiif.io/api/image/2/level2.json"
        }
      ]
    }
  ],
  "items": [''')
f.write(",".join(items))
f.write('''  ]
}''')
f.close()

print items
